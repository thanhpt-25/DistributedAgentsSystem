# Requirements Document

## Introduction

The Distributed Agent System is a Python-based platform in which AI agents run as independent processes on separate machines and cooperate to answer user prompts. A group of stateless Manager Agents accepts a user prompt, decomposes it into a dependency graph of Jobs, dispatches ready Jobs through a message queue to Worker Agents, collects the Job Results, and summarizes them into a Final Result returned to the user.

Version 1 scope (confirmed with the user):

- Language: Python
- LLM access: pluggable provider interface with adapters for OpenAI, Anthropic, AWS Bedrock, and Ollama
- Transports: RabbitMQ (job distribution) and gRPC (control and direct agent calls) behind a pluggable transport interface; HTTP and Kafka adapters are deferred to a later version
- Manager coordination: stateless Manager Agents sharing state through Redis, with lease-based ownership of Requests
- Job model: Jobs form a directed acyclic graph (DAG); a Job may consume the outputs of Jobs it depends on
- Worker routing: by declared Capabilities
- Reliability: Job retries with a dead-letter queue
- Security: API key authentication on the user-facing HTTP API
- Worker tasks: LLM-only (no code execution or tool sandbox in v1)
- Deployment: Docker Compose for local multi-node deployment

## Glossary

- **System**: The Distributed Agent System as a whole.
- **Gateway_API**: The user-facing HTTP API that accepts Requests and returns Final Results.
- **Request**: A single user prompt submitted through the Gateway_API, identified by a unique Request_ID.
- **Request_Status**: The lifecycle status of a Request: Pending, Running, Completed, Failed, or Cancelled. Completed, Failed, and Cancelled are terminal.
- **Manager_Agent**: A stateless process that owns Requests, plans Jobs, dispatches Jobs, and summarizes Job Results.
- **Manager_Group**: The set of all running Manager_Agent instances.
- **Planner**: The Manager_Agent component that uses an LLM to decompose a Request into a Job_Plan.
- **Job_Plan**: A DAG of Jobs produced by the Planner for one Request.
- **Job**: A unit of work with a unique Job_ID, an instruction, a list of required Capabilities, and a list of dependency Job_IDs.
- **Job_State**: The lifecycle state of a Job: Waiting, Ready, Dispatched, Succeeded, Failed, TimedOut, Skipped, or Cancelled. Succeeded, Failed, Skipped, and Cancelled are terminal; TimedOut records a timed-out attempt and leads either to a re-dispatch or to Failed once the maximum attempts are reached.
- **Ready_Job**: A Job whose dependency Jobs have all reached the Succeeded state.
- **Job_Result**: The output produced by a Worker_Agent for one Job, including status, content, and error details.
- **Summarizer**: The Manager_Agent component that uses an LLM to combine Job_Results into a Final_Result.
- **Final_Result**: The answer returned to the user for a Request.
- **Worker_Agent**: A process that consumes Jobs matching its Capabilities, executes the Jobs with an LLM, and publishes Job_Results.
- **Capability**: A named skill label (for example `research`, `summarize`, `code-review`) declared by a Worker_Agent and required by a Job.
- **Agent_Registry**: The Redis-backed registry of live Manager_Agents and Worker_Agents, their Capabilities, and their heartbeats.
- **State_Store**: The Redis instance holding Request, Job_Plan, Job, Job_Result, and lease state.
- **Lease**: A time-bounded exclusive claim, stored in the State_Store, by which one Manager_Agent owns one Request.
- **Transport**: The pluggable communication abstraction used by agents to exchange Messages.
- **Queue_Transport**: The Transport implementation backed by RabbitMQ.
- **RPC_Transport**: The Transport implementation backed by gRPC.
- **Message**: A typed envelope (Job dispatch, Job_Result, heartbeat, or control command) exchanged over a Transport.
- **Message_Codec**: The component that serializes Messages to bytes and parses bytes into Messages.
- **Dead_Letter_Queue**: The RabbitMQ queue receiving Jobs that exhausted their retry budget.
- **LLM_Provider**: The pluggable interface through which agents call a large language model.
- **Provider_Adapter**: An LLM_Provider implementation for one vendor (OpenAI, Anthropic, AWS Bedrock, or Ollama).
- **Agent_Config**: The configuration (file plus environment variables) that defines an agent's role, Transport settings, LLM_Provider settings, Capabilities, and limits.
- **API_Key**: A secret token that authorizes a client to call the Gateway_API.

## Requirements

### Requirement 1: Submit a Request

**User Story:** As a user, I want to submit a prompt to the system over HTTP, so that the agent group can work on it and return an answer.

#### Acceptance Criteria

1. WHEN a client submits a prompt of 1 to the configured maximum length of characters together with a valid API_Key, THE Gateway_API SHALL create a Request with a Request_ID that is unique across all Requests in the State_Store, persist the Request in the State_Store with status Pending, and return the Request_ID to the client within 500 ms of receiving the call.
2. WHEN a client queries an existing Request_ID with a valid API_Key, THE Gateway_API SHALL return within 500 ms the Request status (one of Pending, Running, Completed, Failed, or Cancelled), the Job_ID and status of each Job in the Job_Plan (an empty list if no Job_Plan exists yet), and the Final_Result if and only if the Request status is Completed.
3. IF a client submits a prompt, queries a Request, or cancels a Request with a missing API_Key or an API_Key that does not match a configured API_Key, THEN THE Gateway_API SHALL reject the call with HTTP status 401, create no Request, and leave the state of every existing Request and Job unchanged.
4. IF a client submits a prompt that is empty, consists only of whitespace characters, or is longer than the configured maximum length (default 32,000 characters, configurable in Agent_Config from 1 to 1,000,000 characters), THEN THE Gateway_API SHALL reject the call with HTTP status 400 and an error message identifying which constraint was violated (empty prompt or maximum length exceeded, including the configured maximum), and create no Request.
5. IF a client queries or cancels a Request_ID that does not exist in the State_Store, THEN THE Gateway_API SHALL return HTTP status 404 and change no Request or Job state.
6. WHEN a client cancels a Request whose status is Pending or Running with a valid API_Key, THE System SHALL, within 2 seconds of receiving the call, mark the Request as Cancelled and mark every Job of the Request that is not in the Succeeded, Failed, Skipped, or Cancelled state as Cancelled.
7. IF a Job_Result arrives for a Job of a Request whose status is Cancelled, THEN THE System SHALL discard the Job_Result, keep the Job and Request in the Cancelled state, and dispatch no further Jobs for that Request.
8. IF a client cancels a Request whose status is already Completed, Failed, or Cancelled, THEN THE Gateway_API SHALL reject the call with an error indicating the Request is already in a terminal state and leave the Request, its Jobs, and its Final_Result unchanged.
9. IF the State_Store is unreachable or a write to the State_Store fails while the Gateway_API handles a submit, query, or cancel call, THEN THE Gateway_API SHALL return an error indicating the service is temporarily unavailable within 5 seconds of receiving the call and SHALL NOT return a Request_ID for a Request that was not persisted.

### Requirement 2: Manager Group Ownership of Requests

**User Story:** As an operator, I want any manager agent to be able to pick up and recover requests, so that the system keeps working when a manager machine fails.

#### Acceptance Criteria

1. WHEN a Request is in Pending status and no valid Lease exists for the Request, THE Manager_Agent SHALL attempt to acquire a Lease on the Request within 5 seconds of the Request being persisted in the State_Store, and SHALL NOT invoke the Planner, dispatch Jobs, or change Job states for the Request until the Lease acquisition succeeds.
2. THE State_Store SHALL grant a Lease on a given Request to at most one Manager_Agent at any time, such that when two or more Manager_Agents attempt to acquire the same Lease concurrently, exactly one attempt succeeds and every other attempt is rejected without modifying the Request, Job, or Lease state.
3. THE Lease SHALL record the owning Manager_Agent ID and an expiry time, with a Lease duration read from the Agent_Config in the range 5 to 300 seconds and a default of 30 seconds when the Agent_Config omits the value.
4. WHILE a Manager_Agent holds a Lease, THE Manager_Agent SHALL renew the Lease at an interval shorter than one third of the Lease duration, and each successful renewal SHALL extend the Lease expiry time to the renewal time plus the Lease duration.
5. IF a Lease expires without renewal and at least one other Manager_Agent is running, THEN THE Manager_Group SHALL have another Manager_Agent acquire the Lease within 10 seconds after the expiry time and resume the Request from the Request, Job, and Job_Result state persisted in the State_Store.
6. WHEN a Manager_Agent resumes a Request, THE Manager_Agent SHALL re-dispatch only Jobs that are in Ready or Dispatched state with no Job_Result recorded, SHALL NOT re-dispatch Jobs in Succeeded, Failed, Skipped, or Cancelled state, and SHALL reuse the persisted Job_Results of Succeeded Jobs as dependency inputs and Summarizer inputs.
7. IF a Manager_Agent's Lease renewal fails because the Lease is held by another Manager_Agent, or because the State_Store is unreachable until the Lease expiry time passes, THEN THE Manager_Agent SHALL treat the Lease as lost and SHALL stop issuing dispatches and state transitions for that Request, discarding any in-progress Planner or Summarizer output for that Request without persisting it.
8. IF a Job_Result arrives for a Job that already has a persisted Job_Result in Succeeded state, THEN THE Manager_Agent SHALL retain the first persisted Job_Result and discard the duplicate without changing the Job state.

### Requirement 3: Plan Decomposition

**User Story:** As a user, I want my prompt divided into smaller jobs, so that the work is executed in parallel by specialized workers.

#### Acceptance Criteria

1. WHEN a Manager_Agent acquires a Lease on a Pending Request, THE Planner SHALL produce a Job_Plan containing at least 1 Job and at most the maximum number of Jobs per Job_Plan set in the Agent_Config, where that maximum is an integer from 1 to 100 and defaults to 10 when not set.
2. THE Planner SHALL assign each Job in the Job_Plan a Job_ID that is unique within the Request, a non-empty instruction of at most 8,000 characters, at least one required Capability, and a list of zero or more dependency Job_IDs that excludes the Job's own Job_ID.
3. THE Planner SHALL restrict required Capabilities in the Job_Plan to Capabilities advertised in the Agent_Registry at the moment the Planner sends the planning prompt to the LLM_Provider, and SHALL include that list of advertised Capabilities in the planning prompt.
4. WHEN the Planner receives LLM output for a Job_Plan, THE Planner SHALL parse the output into a Job_Plan object according to the Job_Plan JSON schema and SHALL treat the Job_Plan as valid only if the output conforms to the schema, every dependency Job_ID refers to a Job in the same Job_Plan, the dependency graph contains no cycles, the Job count falls within the bounds of criterion 1, and every required Capability is in the advertised Capability set of criterion 3.
5. IF the LLM output fails any validity check of criterion 4, THEN THE Planner SHALL send the LLM_Provider a corrected-plan request that identifies each failed check, repeating up to the planning retry count set in the Agent_Config, where the retry count is an integer from 0 to 5 and defaults to 2 when not set, for a total of at most (retry count + 1) planning attempts per Request.
6. IF the Planner has not produced a valid Job_Plan after the final planning attempt, THEN THE Manager_Agent SHALL set the Request status to Failed, store an error message in the State_Store that identifies the validity checks the last attempt failed, persist no Job_Plan for the Request, and release the Lease.
7. THE Job_Plan pretty printer SHALL format Job_Plan objects into JSON conforming to the Job_Plan JSON schema.
8. FOR ALL valid Job_Plan objects, printing then parsing then printing SHALL produce an equivalent Job_Plan (round-trip property), where two Job_Plans are equivalent if they contain the same set of Job_IDs and each Job_ID has the same instruction, the same set of required Capabilities, and the same set of dependency Job_IDs.
9. WHEN a valid Job_Plan is produced, THE Manager_Agent SHALL persist the Job_Plan in the State_Store and set the Request status to Running as a single atomic update, so that no reader of the State_Store sees the Request in Running status without its Job_Plan.
10. IF the Agent_Registry advertises zero Capabilities when the Planner starts planning a Request, THEN THE Manager_Agent SHALL set the Request status to Failed with an error message indicating that no Worker_Agent Capabilities are available, make no LLM_Provider call for that Request, and release the Lease.
11. IF the LLM_Provider returns an error for a planning call after its own retries are exhausted, THEN THE Manager_Agent SHALL set the Request status to Failed with an error message that includes the provider, model, and failure reason returned by the LLM_Provider, persist no Job_Plan, and release the Lease.
12. IF the Manager_Agent loses the Lease on a Request before the Job_Plan is persisted, THEN THE Manager_Agent SHALL discard the Job_Plan for that Request and make no change to the Request status or Job_Plan in the State_Store.

### Requirement 4: Job Dispatch Through the Queue

**User Story:** As an operator, I want jobs delivered through a queue to worker agents, so that work is load-balanced and decoupled from manager availability.

#### Acceptance Criteria

1. WHEN a Job becomes a Ready_Job, THE Manager_Agent SHALL set the Job state to Ready, publish exactly one Job dispatch Message for that Job to the Queue_Transport, routed by the complete list of the Job's required Capabilities, and SHALL set the Job state to Dispatched only after the Queue_Transport confirms the publish.
2. WHEN a Job is dispatched, THE Manager_Agent SHALL include in the Job dispatch Message the Job_Result content of every dependency Job, each labelled with the dependency Job's Job_ID, so that the number of included Job_Results equals the number of dependency Job_IDs listed on the Job.
3. WHILE a Job has at least one dependency Job not in Succeeded state, THE Manager_Agent SHALL keep the Job in Waiting state and SHALL NOT publish a Job dispatch Message for that Job.
4. IF a dependency Job reaches the Failed state, THEN THE Manager_Agent SHALL mark every Job that transitively depends on the failed Job as Skipped, SHALL NOT publish a Job dispatch Message for any Skipped Job, and SHALL leave the state of Jobs that do not depend on the failed Job unchanged.
5. THE Queue_Transport SHALL deliver each Job dispatch Message to at most one Worker_Agent at a time, and only to a Worker_Agent whose declared Capabilities include every Capability required by the Job, and SHALL remove the Message from the queue only after that Worker_Agent acknowledges it.
6. IF no live Worker_Agent in the Agent_Registry advertises every Capability required by a Ready_Job for the entire configured capability wait period (default 60 seconds, configurable from 1 to 3600 seconds in the Agent_Config), THEN THE Manager_Agent SHALL mark the Job as Failed with reason "no capable worker" and SHALL NOT publish a Job dispatch Message for that Job.
7. IF a Worker_Agent's connection to the Queue_Transport closes before the Worker_Agent acknowledges a delivered Job dispatch Message, THEN THE Queue_Transport SHALL return the unacknowledged Message to the queue and make it available for delivery to another Worker_Agent with the required Capabilities.
8. IF the Queue_Transport rejects or fails to confirm the publish of a Job dispatch Message, THEN THE Manager_Agent SHALL retry the publish up to 3 times, keep the Job in Ready state between attempts, and mark the Job as Failed with a reason indicating a dispatch failure after the final failed attempt.

### Requirement 5: Worker Job Execution

**User Story:** As an operator, I want worker agents on separate machines to execute jobs with an LLM, so that capacity scales horizontally by adding machines.

#### Acceptance Criteria

1. WHEN a Worker_Agent starts, THE Worker_Agent SHALL register in the Agent_Registry its agent ID (unique across all running agents), host, the Capabilities declared in its Agent_Config (1 to 50 Capabilities), and its concurrency limit (an integer from 1 to 100 taken from its Agent_Config) before consuming any Job dispatch Message.
2. WHEN a Worker_Agent completes registration in the Agent_Registry, THE Worker_Agent SHALL subscribe to exactly one Queue_Transport queue for each of its declared Capabilities and to no queue for any Capability it has not declared.
3. WHILE a Worker_Agent is executing a number of Jobs equal to its concurrency limit, THE Worker_Agent SHALL leave additional Job dispatch Messages unconsumed in the Queue_Transport and SHALL resume consumption within 1 second after the number of executing Jobs drops below the concurrency limit.
4. WHEN a Worker_Agent receives a Job dispatch Message, THE Worker_Agent SHALL send to the configured LLM_Provider one completion call containing the Job instruction and the content of every dependency Job_Result included in the Job dispatch Message.
5. WHEN a Worker_Agent finishes a Job, THE Worker_Agent SHALL publish a Job_Result Message containing the Job_ID, attempt number, Worker_Agent ID, status (Succeeded, Failed, TimedOut, or Cancelled), content, token usage, and execution duration in milliseconds, and SHALL acknowledge the Job dispatch Message only after the Queue_Transport confirms publication of the Job_Result Message.
6. IF a Job execution exceeds the configured Job timeout (an integer from 1 to 3600 seconds, default 300 seconds, measured from the start of execution), THEN THE Worker_Agent SHALL abort the execution within 5 seconds of the timeout and publish a Job_Result with status TimedOut and an error reason indicating the timeout duration.
7. WHEN a Worker_Agent receives a cancellation control command for a Job_ID that the Worker_Agent is executing, THE Worker_Agent SHALL abort the execution within 5 seconds and publish a Job_Result with status Cancelled.
8. IF the LLM_Provider returns an error for a Job's completion call, THEN THE Worker_Agent SHALL publish a Job_Result with status Failed, empty content, and an error reason containing the provider, model, and failure reason returned by the LLM_Provider.
9. IF a Worker_Agent receives a Job dispatch Message that the Message_Codec cannot parse or that lacks a Job_ID or instruction, THEN THE Worker_Agent SHALL reject the Message without requeueing so that the Queue_Transport routes it to the Dead_Letter_Queue, and SHALL continue consuming other Job dispatch Messages.
10. IF a Worker_Agent fails to register in the Agent_Registry within 30 seconds of starting, THEN THE Worker_Agent SHALL exit with a non-zero exit code and an error indicating the registration failure, without subscribing to any Queue_Transport queue.

### Requirement 6: Retries and Dead-Lettering

**User Story:** As an operator, I want failed jobs retried and poison jobs isolated, so that transient failures do not fail whole requests and broken jobs do not block the queue.

#### Acceptance Criteria

1. IF a Job_Result reports status Failed or TimedOut, the Job_Result attempt number equals the Job's current attempt count, and the current attempt count is below the configured maximum attempts (integer from 1 to 10, default 3, where the first dispatch of a Job is attempt 1), THEN THE Manager_Agent SHALL record the Job state as Failed or TimedOut for that attempt, re-dispatch the Job with the attempt count incremented by 1, with the same Job_ID, instruction, required Capabilities, and dependency Job_Results as the previous attempt, and set the Job state to Dispatched.
2. WHEN the Manager_Agent schedules a re-dispatch of a Job, THE Manager_Agent SHALL wait a backoff delay of no less than the configured base backoff delay (0.1 to 60 seconds, default 2 seconds) multiplied by 2^(N−1), where N is the attempt number that failed, capped at the configured maximum backoff delay (1 to 600 seconds, default 60 seconds), before publishing the Job dispatch Message.
3. IF a Job reaches the configured maximum attempts without a Succeeded Job_Result, THEN THE Manager_Agent SHALL mark the Job as Failed, store in the State_Store the final attempt count and the error reason from the last Job_Result, and THE Queue_Transport SHALL route the Job dispatch Message to the Dead_Letter_Queue with the Request_ID, Job_ID, final attempt count, and last error reason.
4. IF a Worker_Agent's connection to the Queue_Transport closes before the Worker_Agent acknowledges a Job dispatch Message, THEN THE Queue_Transport SHALL return the Job dispatch Message to its Capability queue and redeliver the Job dispatch Message to a live Worker_Agent subscribed to that queue, with the Job attempt count unchanged.
5. WHEN the Manager_Agent receives a Job_Result whose Job_ID and attempt number already have a recorded Job_Result, or whose attempt number is lower than the Job's current attempt count, THE Manager_Agent SHALL discard the Job_Result and leave the Job state, attempt count, and stored Job_Results unchanged (idempotent result handling).
6. IF a Job dispatch Message has been redelivered after unacknowledged delivery a number of times equal to the configured maximum attempts, THEN THE Queue_Transport SHALL route the Job dispatch Message to the Dead_Letter_Queue instead of redelivering it, and THE Manager_Agent SHALL mark the Job as Failed with an error reason indicating repeated unacknowledged delivery.
7. IF a Job_Result reports status Cancelled, THEN THE Manager_Agent SHALL set the Job state to Cancelled and SHALL NOT re-dispatch the Job or route the Job dispatch Message to the Dead_Letter_Queue.
8. IF an Agent_Config specifies a maximum attempts, base backoff delay, or maximum backoff delay value outside its permitted range, or a base backoff delay greater than the maximum backoff delay, THEN THE Manager_Agent SHALL abort startup with an error naming the invalid setting and its permitted range.

### Requirement 7: Result Summarization

**User Story:** As a user, I want a single coherent answer, so that I do not need to read every job's output.

#### Acceptance Criteria

1. WHEN every Job in a Job_Plan has reached a terminal state (Succeeded, Failed, Skipped, or Cancelled) and at least one Job has reached Succeeded state, THE Summarizer SHALL produce exactly one Final_Result through the LLM_Provider, using the original prompt of the Request and the content of every Succeeded Job_Result in the Job_Plan as input, and using no Job_Result content from Jobs in Failed, Skipped, or Cancelled state.
2. WHEN the Summarizer produces a Final_Result for a Job_Plan in which at least one Job ended in Failed, Skipped, or Cancelled state, THE Summarizer SHALL include in the Final_Result a list containing one entry for each such Job, where each entry states the Job_ID, the terminal state, and the error reason recorded in the Job_Result (or, for a Skipped or Cancelled Job with no Job_Result, a reason identifying the Job_ID of the dependency or the cancellation that caused the Job not to run).
3. IF every Job in the Job_Plan has reached a terminal state and no Job has reached Succeeded state, THEN THE Manager_Agent SHALL skip the Summarizer, store as the Final_Result in the State_Store a list of every Job's Job_ID, terminal state, and error reason, set the Request status to Failed, and release the Lease.
4. WHEN the Summarizer produces a Final_Result, THE Manager_Agent SHALL persist the Final_Result in the State_Store, then set the Request status to Completed only after the persist succeeds, then release the Lease, so that a Request with status Completed always has a retrievable Final_Result.
5. THE Final_Result SHALL include one entry per Job in the Job_Plan stating the Job_ID, the terminal state, the ID of the Worker_Agent that produced the Job_Result (or an indication that no Worker_Agent ran the Job), and the input and output token counts reported in the Job_Result (or zero if no Job_Result exists), plus the input and output token counts consumed by the Summarizer.
6. IF the Summarizer's LLM_Provider call returns an error or does not return a response within 120 seconds, THEN THE Manager_Agent SHALL retry the Summarizer call up to 2 additional times, and IF all 3 attempts fail, THEN THE Manager_Agent SHALL store as the Final_Result the list described in criterion 2 plus an error reason indicating that summarization failed, set the Request status to Failed, and release the Lease, retaining all stored Job_Results unchanged in the State_Store.
7. IF persisting the Final_Result or the Request status to the State_Store fails, THEN THE Manager_Agent SHALL retry the write up to 3 additional times at 1-second intervals while continuing to hold the Lease, and IF all 4 attempts fail, THEN THE Manager_Agent SHALL leave the Request status unchanged and stop renewing the Lease so that the Request becomes claimable by another Manager_Agent after the Lease expires.

### Requirement 8: Pluggable Transport

**User Story:** As a developer, I want communication to go through a transport abstraction, so that RabbitMQ, gRPC, HTTP, or Kafka can be used without changing agent logic.

#### Acceptance Criteria

1. THE Transport SHALL expose four operations with identical signatures across all Transport implementations: publish (send one Message to a named destination), subscribe (register a handler that receives Messages from a named destination), acknowledge (confirm that one received Message has been processed), and request-reply (send one Message to a named destination and return the single reply Message to the caller), so that agent logic calls no protocol-specific API.
2. THE System SHALL provide a Queue_Transport implementation backed by RabbitMQ that carries Job dispatch Messages and Job_Result Messages.
3. THE System SHALL provide an RPC_Transport implementation backed by gRPC that carries control command Messages and direct agent-to-agent request-reply calls.
4. WHERE an Agent_Config selects a Transport implementation by name, THE System SHALL load the selected implementation during agent startup, before the agent subscribes to any destination or publishes any Message.
5. IF an Agent_Config names a Transport implementation that is not installed, THEN THE System SHALL abort agent startup with a non-zero exit status and an error message containing the name of the missing implementation, without subscribing to any destination or publishing any Message.
6. IF a Transport connection drops, THEN THE Transport SHALL attempt reconnection starting at a 1 second interval, doubling the interval after each failed attempt up to the configured maximum interval (configurable from 1 to 300 seconds, default 30 seconds), and, after a successful reconnection, SHALL re-establish every subscription that was active before the drop before delivering any further Message to subscribers.
7. IF a request-reply call receives no reply within the configured request-reply timeout (configurable from 1 to 300 seconds, default 30 seconds), THEN THE Transport SHALL return a timeout error to the caller identifying the destination of the call.
8. IF publish or request-reply is invoked while the Transport connection is down, THEN THE Transport SHALL return an error to the caller indicating that the Transport is unavailable, and SHALL NOT report the Message as sent.
9. IF a subscriber does not acknowledge a received Message before its connection drops, THEN THE Queue_Transport SHALL make that Message available for redelivery to a subscriber of the same destination.

### Requirement 9: Message Serialization

**User Story:** As a developer, I want a single well-defined message format, so that agents written against different transports interoperate.

#### Acceptance Criteria

1. THE Message_Codec SHALL serialize every Message into an envelope containing exactly these fields: message type (one of Job dispatch, Job_Result, heartbeat, or control command), schema version (a positive integer), message ID (unique per Message, at most 64 characters), correlation ID (the Request_ID for Job dispatch and Job_Result Messages; empty for heartbeat and control command Messages not tied to a Request), timestamp (UTC, millisecond precision, recording when the Message was created), and payload.
2. WHEN the Message_Codec receives bytes conforming to the Message schema with a supported schema version, THE Message_Codec SHALL parse the bytes into the typed Message object that matches the envelope's message type, with all envelope fields and payload fields populated from the bytes.
3. THE Message_Codec SHALL produce byte-identical output for the same Message object regardless of whether the Message is sent over the Queue_Transport or the RPC_Transport.
4. IF the Message_Codec receives bytes that are not parseable, are missing any required envelope field, contain an unrecognized message type, contain a payload that does not match the schema for the declared message type, or carry a schema version outside the set of supported schema versions, THEN THE Message_Codec SHALL return a parse error that identifies the failure reason and the offending field, and SHALL NOT return a partially populated Message object.
5. IF the Message_Codec returns a parse error for a received Message, THEN THE receiving agent SHALL reject the Message without requeueing it, SHALL leave its State_Store data unchanged as a result of that Message, and SHALL continue processing subsequent Messages.
6. IF a serialized Message exceeds the maximum Message size configured in Agent_Config (default 1 MiB), THEN THE Message_Codec SHALL return a serialization error indicating the size limit was exceeded and SHALL NOT produce output bytes, and IF received bytes exceed that size, THEN THE Message_Codec SHALL return a parse error indicating the size limit was exceeded.
7. FOR ALL valid Message objects of every message type, serializing then parsing SHALL produce a Message object equal to the original in every envelope field and payload field (round-trip property).

### Requirement 10: Pluggable LLM Provider

**User Story:** As a developer, I want to swap LLM vendors per agent, so that I can balance cost, quality, and data locality.

#### Acceptance Criteria

1. THE LLM_Provider SHALL expose a completion operation that accepts a system prompt, a list of 1 to 100 messages (each with a role of system, user, or assistant and text content), model parameters (model name, temperature from 0.0 to 2.0, and maximum output tokens from 1 to the selected model's output limit), and a timeout from 1 to 600 seconds (default 60 seconds), and returns the generated text content plus token usage reported as input token count, output token count, and total token count.
2. THE System SHALL provide Provider_Adapters for OpenAI, Anthropic, AWS Bedrock, and Ollama, each implementing the same LLM_Provider completion operation, so that changing the Provider_Adapter in Agent_Config requires no change to agent code.
3. WHERE an Agent_Config selects a Provider_Adapter and model, THE agent SHALL route 100% of its LLM calls (Planner, Summarizer, and Job execution calls made by that agent) through the selected Provider_Adapter and model.
4. IF a Provider_Adapter call returns a rate-limit error, a transient network error (connection failure, connection reset, or server-side unavailability), or exceeds the per-call timeout, THEN THE LLM_Provider SHALL retry the call with exponential backoff starting at 1 second, doubling after each attempt, capped at 30 seconds per wait, up to the configured retry count (from 0 to 10, default 3).
5. IF a Provider_Adapter call fails after the configured retry count is exhausted, THEN THE LLM_Provider SHALL return an error to the caller identifying the provider, model, failure reason, and number of attempts made, and SHALL return no partial content.
6. IF a Provider_Adapter call returns a non-transient error (authentication failure, authorization failure, invalid request, unknown model, or input exceeding the model's context limit), THEN THE LLM_Provider SHALL return an error identifying the provider, model, and failure reason without retrying the call.
7. IF an Agent_Config names a Provider_Adapter other than OpenAI, Anthropic, AWS Bedrock, or Ollama, omits the model name, or omits credentials required by the selected Provider_Adapter, THEN THE agent SHALL fail to start, exit with a non-zero exit status, and report an error identifying the invalid or missing configuration setting.
8. IF the retry count or timeout in Agent_Config is outside its allowed range (retry count 0 to 10, timeout 1 to 600 seconds), THEN THE agent SHALL fail to start, exit with a non-zero exit status, and report an error identifying the out-of-range setting and its allowed range.

### Requirement 11: Agent Registry and Health

**User Story:** As an operator, I want to see which agents are alive and what they can do, so that routing and planning use only healthy workers.

#### Acceptance Criteria

1. WHILE an agent is running, THE agent SHALL publish a heartbeat to the Agent_Registry once per heartbeat interval, where the heartbeat interval is read from Agent_Config, accepts values from 1 to 60 seconds, and defaults to 5 seconds when not configured.
2. IF the Agent_Registry receives no heartbeat from an agent for three consecutive heartbeat intervals, THEN THE Agent_Registry SHALL mark the agent as Unavailable within one additional heartbeat interval and remove from the advertised Capability set every Capability of that agent that is not also declared by at least one other agent in the Available status.
3. WHEN an agent receives a graceful shutdown signal, THE agent SHALL stop accepting new Jobs immediately, finish in-flight Jobs, requeue every in-flight Job not finished when the shutdown timeout elapses, deregister from the Agent_Registry, and close all Transport connections, completing all steps within the shutdown timeout read from Agent_Config (accepted range 1 to 300 seconds, default 30 seconds).
4. THE Gateway_API SHALL expose an endpoint, authorized by a valid API_Key, that lists every agent present in the Agent_Registry (including Unavailable agents) with agent identifier, role (Manager_Agent or Worker_Agent), host, declared Capabilities, status (Available or Unavailable), and last heartbeat time as a UTC timestamp with second precision.
5. THE System SHALL expose a health-check endpoint on each agent that responds within 5 seconds and reports, separately for Transport, State_Store, and LLM_Provider, a status of connected or disconnected, plus an overall status of healthy only when all three are connected and unhealthy otherwise, reporting any dependency whose connectivity check does not complete within 2 seconds as disconnected.
6. WHEN an agent starts, THE agent SHALL register with the Agent_Registry its agent identifier, role, host, and declared Capabilities, with status Available, before consuming any Job or Request.
7. WHEN the Agent_Registry receives a heartbeat from an agent marked Unavailable, THE Agent_Registry SHALL mark the agent as Available and add the agent's declared Capabilities back to the advertised Capability set.
8. IF a call to the agent-listing endpoint has a missing or invalid API_Key, THEN THE Gateway_API SHALL reject the call with an error response indicating authentication failure and return no agent data.

### Requirement 12: Configuration

**User Story:** As an operator, I want each agent configured independently, so that I can run different roles on different machines.

#### Acceptance Criteria

1. WHEN an agent starts, THE agent SHALL load Agent_Config from the configuration file whose location is supplied at startup, and SHALL resolve each field with the precedence order: matching environment variable first, then configuration file value, then documented default value.
2. IF the Agent_Config is missing a required field (role, Transport settings, LLM_Provider settings, or, for a Worker_Agent role, at least 1 Capability) or contains a value that fails type or range validation, THEN THE agent SHALL abort startup with a non-zero exit status before connecting to any Transport, the State_Store, or an LLM_Provider, and SHALL emit an error listing every invalid or missing field by name together with the reason each field failed validation.
3. IF the configuration file does not exist, cannot be read, or cannot be parsed, THEN THE agent SHALL abort startup with a non-zero exit status and emit an error identifying the configuration file location and the reason for the failure.
4. THE agent SHALL read API_Keys and LLM credentials only from environment variables or from a secrets file referenced by Agent_Config.
5. IF the configuration file itself contains an API_Key or LLM credential value, THEN THE agent SHALL abort startup with a non-zero exit status and emit an error naming the offending field without including the credential value.
6. IF a secrets file referenced by Agent_Config does not exist or cannot be read, THEN THE agent SHALL abort startup with a non-zero exit status and emit an error identifying the secrets file reference without including any credential value.
7. THE System SHALL replace every occurrence of an API_Key or LLM credential value in all log output, including error messages and exception traces, with a fixed redaction marker, such that no log line contains the full credential value or any substring of 8 or more consecutive characters of it.

### Requirement 13: Observability

**User Story:** As an operator, I want to trace a request across machines, so that I can debug slow or failed requests.

#### Acceptance Criteria

1. THE System SHALL write every log entry from every Manager_Agent, Worker_Agent, and the Gateway_API as a single-line JSON object that contains, at minimum, a timestamp in UTC ISO 8601 format with millisecond precision, a log level, the emitting agent ID, and a message field.
2. WHEN a log entry is emitted while processing a specific Request, THE System SHALL include the Request_ID in the log entry.
3. WHEN a log entry is emitted while processing a specific Job, THE System SHALL include both the Job_ID and the Request_ID of the owning Request in the log entry.
4. THE System SHALL propagate the Request_ID as a correlation ID in every Message related to the Request, covering Job dispatch, Job_Result, and control command Messages sent over both the Queue_Transport and the RPC_Transport.
5. WHEN an agent receives a Message carrying a correlation ID, THE agent SHALL use that correlation ID as the Request_ID in every log entry and every outbound Message it produces while handling the received Message.
6. IF an agent receives a Job dispatch, Job_Result, or control command Message for a Request without a correlation ID, THEN THE agent SHALL emit a log entry at warning level that identifies the Message type and sending agent ID, and SHALL continue processing the Message.
7. WHEN a Job changes state (Waiting, Ready, Dispatched, Succeeded, Failed, TimedOut, Skipped, or Cancelled), THE Manager_Agent SHALL record in the State_Store, in the same update as the state change, an entry holding the Job_ID, the previous state, the new state, the attempt number, the Worker_Agent ID when known, and a UTC timestamp with millisecond precision.
8. THE State_Store SHALL keep all recorded Job state transition entries for a Request, in chronological order, until the Request record itself is removed, so that no transition entry is lost when a Lease moves to another Manager_Agent.
9. WHEN a client queries a Request_ID with a valid API_Key, THE Gateway_API SHALL return, along with the Request status, the chronologically ordered Job state transition entries recorded for that Request.
10. THE System SHALL exclude API_Keys, LLM credentials, and full prompt or Job_Result content from structured log entries.

### Requirement 14: Local Multi-Node Deployment

**User Story:** As a developer, I want to run the whole system locally with multiple containers, so that I can test distributed behavior before deploying to separate machines.

#### Acceptance Criteria

1. THE System SHALL provide a Docker Compose definition that starts one RabbitMQ container, one Redis container, one Gateway_API container, at least two Manager_Agent containers, and at least two Worker_Agent containers, where the Worker_Agent containers declare at least two distinct Capabilities in total and no Worker_Agent container declares a Capability set identical to another's.
2. WHEN a developer starts the Docker Compose definition on a host with the container images available locally, THE System SHALL bring every defined container to a running state, register every Manager_Agent and Worker_Agent in the Agent_Registry, and accept an authenticated Request through the Gateway_API within 120 seconds of the start command.
3. THE System SHALL provide one container image per agent role (Gateway_API, Manager_Agent, Worker_Agent), where the agent's role and Capabilities are determined only by Agent_Config, so that two containers built from the same image with different Agent_Config values declare different Capabilities.
4. IF an agent container starts with an Agent_Config that is missing a role, specifies an unrecognized role, or declares a Worker_Agent with zero Capabilities, THEN THE agent SHALL exit with a non-zero exit code and an error message identifying the invalid or missing setting, without registering in the Agent_Registry.
5. WHEN a Worker_Agent or Manager_Agent container is added through Docker Compose scaling, THE new agent SHALL register in the Agent_Registry with its role and Capabilities within 2 heartbeat intervals of container start and SHALL become eligible to receive Jobs (for a Worker_Agent) or Requests (for a Manager_Agent) without any already-running agent being restarted.
6. WHEN a Worker_Agent or Manager_Agent container is removed through Docker Compose scaling, THE removed agent SHALL perform graceful shutdown as defined in Requirement 11 criterion 3, and THE remaining agents SHALL continue processing in-flight and new Requests without being restarted.
7. IF RabbitMQ or Redis is not reachable when an agent container starts, THEN THE agent SHALL retry the connection at the configured retry interval for up to the configured startup timeout (default 60 seconds), and IF the dependency remains unreachable after that timeout, THEN THE agent SHALL exit with a non-zero exit code and an error message naming the unreachable dependency.
8. THE System SHALL support running each agent role on a separate host by reading the network addresses (host and port) of RabbitMQ, Redis, and gRPC endpoints exclusively from Agent_Config, such that no address is hard-coded to a Docker Compose service name or to localhost.

### Requirement 15: Deferred Scope (Future Versions)

**User Story:** As a product owner, I want deferred capabilities recorded, so that the v1 design leaves room for them.

#### Acceptance Criteria

1. WHERE an HTTP Transport adapter that implements the publish, subscribe, acknowledge, and request-reply operations defined in Requirement 8 is installed AND an Agent_Config selects it by name, THE System SHALL load and use the adapter at agent startup with zero modifications to Manager_Agent or Worker_Agent source code.
2. WHERE a Kafka Transport adapter that implements the publish, subscribe, acknowledge, and request-reply operations defined in Requirement 8 is installed AND an Agent_Config selects it by name, THE System SHALL load and use the adapter at agent startup with zero modifications to Manager_Agent or Worker_Agent source code.
3. THE System SHALL ship v1 with no HTTP or Kafka Transport adapter, so that an Agent_Config selecting either name in v1 is handled by the missing-implementation behavior of Requirement 8 criterion 5 (agent startup aborts with an error naming the missing implementation).
4. THE Manager_Agent and Worker_Agent SHALL exchange Messages only through the Transport interface operations defined in Requirement 8 and the Message_Codec defined in Requirement 9, with no direct calls to RabbitMQ or gRPC client libraries from Manager_Agent or Worker_Agent logic.
5. IF an installed Transport adapter selected by an Agent_Config does not implement all four Transport operations (publish, subscribe, acknowledge, request-reply), THEN THE System SHALL abort agent startup with an error naming the adapter and each missing operation, and SHALL NOT register the agent in the Agent_Registry.

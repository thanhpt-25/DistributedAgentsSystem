import type { Priority, SubmitBody } from '../api/types'

export interface SubmitForm {
  prompt: string
  priority: string
  tokens: string
  cost: string
}

/**
 * The request body for POST /v1/requests: only the fields the user filled in, so an absent priority falls back to the
 * tenant default and an absent budget to the key's default budget. `max_cost` stays a string, as the Gateway parses it
 * to an exact Decimal.
 */
export function buildSubmitBody({ prompt, priority, tokens, cost }: SubmitForm): SubmitBody {
  const budget = {
    ...(tokens ? { max_total_tokens: Number(tokens) } : {}),
    ...(cost ? { max_cost: cost } : {}),
  }
  return {
    prompt,
    ...(priority ? { priority: priority as Priority } : {}),
    ...(Object.keys(budget).length ? { budget } : {}),
  }
}

{{/*
Helpers of the DAS chart.
*/}}

{{/* Name of the chart, overridable. */}}
{{- define "das.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified name: the release name, with the chart name appended unless it is already part of it. */}}
{{- define "das.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Labels common to every object. */}}
{{- define "das.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "das.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels of one component: dict "root" . "component" "gateway". Stable: they never change on upgrade. */}}
{{- define "das.selectorLabels" -}}
app.kubernetes.io/name: {{ include "das.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Common labels plus the component. */}}
{{- define "das.componentLabels" -}}
{{ include "das.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Image of one role: <repository>-<role>:<tag>, the tag defaulting to the chart appVersion. */}}
{{- define "das.image" -}}
{{- printf "%s-%s:%s" .root.Values.image.repository .role (default .root.Chart.AppVersion .root.Values.image.tag) -}}
{{- end -}}

{{/* ServiceAccount the pods run as: the one the chart creates, a named existing one, or "default". */}}
{{- define "das.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "das.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
The agent config file of one role (dict "root" . "role" "gateway|manager|worker"): built-in defaults, then
config.common, then config.<role>, then the role itself, each layer overriding the one before. No secret ever goes here.
*/}}
{{- define "das.config" -}}
{{- $base := dict "health_port" 8081 "transports" (dict "jobs" "rabbitmq" "control" "grpc") "grpc" (dict "bind" "0.0.0.0:50051" "advertise_port" 50051) -}}
{{- if eq .role "gateway" -}}{{- $_ := set $base "http_port" 8080 -}}{{- end -}}
{{- $common := deepCopy (default (dict) .root.Values.config.common) -}}
{{- $own := deepCopy (default (dict) (index .root.Values.config .role)) -}}
{{- toYaml (mustMergeOverwrite (dict) $base $common $own (dict "role" .role)) -}}
{{- end -}}

{{/*
Environment of one role's container (dict "root" . "role" ...): the pod's own address and name, the credential-free
connection values, and every secretEnv entry as a secretKeyRef into existingSecret. A plain value replaces the
secretEnv entry of the same name. Only the connection secrets are required; the rest are optional keys.
*/}}
{{- define "das.env" -}}
{{- $root := .root -}}
{{- $plain := dict -}}
{{- if $root.Values.redis.url -}}{{- $_ := set $plain "DAS__REDIS__URL" $root.Values.redis.url -}}{{- end -}}
{{- if and $root.Values.rabbitmq.url (ne .role "gateway") -}}{{- $_ := set $plain "DAS__RABBITMQ__URL" $root.Values.rabbitmq.url -}}{{- end -}}
{{- if and $root.Values.rabbitmq.managementUrl (eq .role "manager") -}}{{- $_ := set $plain "DAS__RABBITMQ__MANAGEMENT_URL" $root.Values.rabbitmq.managementUrl -}}{{- end -}}
- name: DAS__GRPC__ADVERTISE_HOST
  valueFrom:
    fieldRef:
      fieldPath: status.podIP
- name: DAS__AGENT_ID
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
{{- range $name, $value := $plain }}
- name: {{ $name }}
  value: {{ $value | quote }}
{{- end }}
{{- range $name, $key := (default (dict) (index $root.Values.secretEnv .role)) }}
{{- if not (hasKey $plain $name) }}
- name: {{ $name }}
  valueFrom:
    secretKeyRef:
      name: {{ $root.Values.existingSecret }}
      key: {{ $key }}
      {{- if not (has $name (list "DAS__API_KEYS" "DAS__REDIS__URL" "DAS__RABBITMQ__URL")) }}
      optional: true
      {{- end }}
{{- end }}
{{- end }}
{{- end -}}

{{/*
The pod spec shared by the three roles (dict "root" . "role" ... "component" ... "resources" {...} "capabilities" "a,b").
*/}}
{{- define "das.podSpec" -}}
serviceAccountName: {{ include "das.serviceAccountName" .root }}
automountServiceAccountToken: false
terminationGracePeriodSeconds: {{ .root.Values.terminationGracePeriodSeconds }}
securityContext:
  seccompProfile:
    type: RuntimeDefault
{{- with .root.Values.imagePullSecrets }}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end }}
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          topologyKey: kubernetes.io/hostname
          labelSelector:
            matchLabels:
              {{- include "das.selectorLabels" (dict "root" .root "component" .component) | nindent 14 }}
containers:
  - name: das
    image: {{ include "das.image" (dict "root" .root "role" .role) | quote }}
    imagePullPolicy: {{ .root.Values.image.pullPolicy }}
    args: ["run", "--config", "/etc/das/config.yaml"]
    env:
      {{- include "das.env" (dict "root" .root "role" .role) | nindent 6 }}
      {{- if .capabilities }}
      - name: DAS__WORKER__CAPABILITIES
        value: {{ .capabilities | quote }}
      {{- end }}
    ports:
      {{- if eq .role "gateway" }}
      - name: http
        containerPort: 8080
      {{- end }}
      - name: health
        containerPort: 8081
      - name: grpc
        containerPort: 50051
    startupProbe:
      httpGet: {path: /livez, port: health}
      periodSeconds: 3
      failureThreshold: 30
    livenessProbe:
      httpGet: {path: /livez, port: health}
      periodSeconds: 10
      failureThreshold: 3
    readinessProbe:
      httpGet: {path: /readyz, port: health}
      periodSeconds: 5
      failureThreshold: 2
    resources:
      {{- toYaml (default (dict) .resources) | nindent 6 }}
    securityContext:
      allowPrivilegeEscalation: false
      capabilities:
        drop: ["ALL"]
    volumeMounts:
      - name: config
        mountPath: /etc/das
        readOnly: true
volumes:
  - name: config
    configMap:
      name: {{ include "das.fullname" .root }}-{{ .role }}
{{- end -}}

{{/*
Refuse to render a grace period shorter than the graceful shutdown (das-multi-tenant Req 10.4).
A pod gets SIGTERM, then SIGKILL after terminationGracePeriodSeconds; the agents need limits.shutdown_timeout to
finish what they run, and 10 seconds of margin for the rest of the shutdown.
*/}}
{{- define "das.validate" -}}
{{- $shutdown := int (dig "common" "limits" "shutdown_timeout" 30 (.Values.config | default dict)) -}}
{{- $minimum := add $shutdown 10 | int -}}
{{- $grace := int .Values.terminationGracePeriodSeconds -}}
{{- if lt $grace $minimum -}}
{{- fail (printf "terminationGracePeriodSeconds is %d but must be at least limits.shutdown_timeout (%d) + 10 = %d, or pods are killed before their graceful shutdown finishes" $grace $shutdown $minimum) -}}
{{- end -}}
{{- end -}}

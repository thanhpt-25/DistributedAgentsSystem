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

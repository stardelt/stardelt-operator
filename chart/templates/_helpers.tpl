{{- define "stardelt-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stardelt-operator.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "stardelt-operator.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "stardelt-operator.labels" -}}
app.kubernetes.io/name: {{ include "stardelt-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: stardelt
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "stardelt-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "stardelt-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

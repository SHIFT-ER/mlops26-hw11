{{- define "sim.labels" -}}
app.kubernetes.io/name: llm-sim
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "sim.selector" -}}
app.kubernetes.io/name: llm-sim
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: backend
{{- end -}}

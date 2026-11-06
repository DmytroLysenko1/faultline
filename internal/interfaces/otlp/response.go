package otlp

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/DmytroLysenko1/faultline/internal/application/ingest"
)

const reasonSeparator = "; "

func responseOf(result ingest.AcceptResult) ptraceotlp.ExportResponse {
	response := ptraceotlp.NewExportResponse()
	if result.Rejected == 0 {
		return response
	}
	partial := response.PartialSuccess()
	partial.SetRejectedSpans(int64(result.Rejected))
	partial.SetErrorMessage(errorMessage(result.Reasons))
	return response
}

func errorMessage(reasons []string) string {
	var message strings.Builder
	for _, reason := range reasons {
		if !appendReason(&message, reason) {
			break
		}
	}
	return message.String()
}

func appendReason(message *strings.Builder, reason string) bool {
	if message.Len() == 0 {
		message.WriteString(truncated(reason))
		return len(reason) <= MaxErrorMessageBytes
	}
	if message.Len()+len(reasonSeparator)+len(reason) > MaxErrorMessageBytes {
		return false
	}
	message.WriteString(reasonSeparator)
	message.WriteString(reason)
	return true
}

func truncated(reason string) string {
	if len(reason) <= MaxErrorMessageBytes {
		return reason
	}
	return strings.ToValidUTF8(reason[:MaxErrorMessageBytes], "")
}

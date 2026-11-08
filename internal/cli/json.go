package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"google.golang.org/protobuf/encoding/protojson"

	faultlinev1 "github.com/DmytroLysenko1/faultline/internal/gen/proto/faultline/v1"
)

const (
	jsonIndent  = "  "
	jsonNewline = '\n'
)

func JSON(w io.Writer, graph *faultlinev1.Graph) error {
	compact, err := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
	}.Marshal(graph)
	if err != nil {
		return fmt.Errorf("marshal graph: %w", err)
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, compact, "", jsonIndent); err != nil {
		return fmt.Errorf("indent graph: %w", err)
	}
	indented.WriteByte(jsonNewline)
	_, err = w.Write(indented.Bytes())
	return err
}

package resource

import "github.com/DmytroLysenko1/faultline/pkg/checkpoint"

func (s ServiceRef) WriteCheckpoint(w *checkpoint.Writer) {
	w.Text(s.Namespace)
	w.Text(s.Name)
}

func ReadServiceRef(r *checkpoint.Reader) ServiceRef {
	read := ServiceRef{
		Namespace: r.Text(MaxServiceNamespaceBytes),
		Name:      r.Text(MaxServiceNameBytes),
	}
	source := Resource{
		ServiceName:      read.Name,
		ServiceNamespace: read.Namespace,
	}
	if err := source.Validate(); err != nil && r.Err() == nil {
		r.Fail(err.Error())
	}
	return read
}

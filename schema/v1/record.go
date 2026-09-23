package schemav1

// Header returns the header of the record in the envelope, allocating it if
// unset, or nil if the envelope is empty.
func (x *Record) Header() *Header {
	var h **Header
	switch b := x.GetBody().(type) {
	case *Record_Sample:
		h = &b.Sample.Header
	case *Record_Event:
		h = &b.Event.Header
	case *Record_ProbeResult:
		h = &b.ProbeResult.Header
	case *Record_Incident:
		h = &b.Incident.Header
	default:
		return nil
	}
	if *h == nil {
		*h = &Header{}
	}
	return *h
}

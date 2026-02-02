package relay

type internalEventType string

const (
	internalEventTypeSubscribed   internalEventType = "subscribed"
	internalEventTypeUnsubscribed internalEventType = "unsubscribed"
)

type internalEvent interface {
	getInternalEventType() internalEventType
}

type baseInternalEvent struct {
	internalEventType internalEventType
}

func (e baseInternalEvent) getInternalEventType() internalEventType {
	return e.internalEventType
}

type internalEventSubscribed struct {
	baseInternalEvent
	TotalSubscriptions int
}

type internalEventUnsubscribed struct {
	baseInternalEvent
	TotalSubscriptions int
}

package stream

import (
	"google.golang.org/grpc"

	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
)

// Generic RPC stream.
type Stream interface {
	grpc.ServerStream

	Send(*discovery.DiscoveryResponse) error
	Recv() (*discovery.DiscoveryRequest, error)
}

type DeltaStream interface {
	grpc.ServerStream

	Send(*discovery.DeltaDiscoveryResponse) error
	Recv() (*discovery.DeltaDiscoveryRequest, error)
}

type SotwSubscribed struct {
	wildcard                bool
	subscribedResourceNames map[string]struct{}
}

func (s *SotwSubscribed) setNames(names []string) {
	for _, v := range names {
		if v == "*" {
			s.wildcard = true
			// don't need to track it explicitly
			s.subscribedResourceNames = make(map[string]struct{})
			break
		} else {
			s.subscribedResourceNames[v] = struct{}{}
		}
	}
}

// StreamState will keep track of resource state per type on a stream.
type StreamState struct { // nolint:golint,revive
	// Indicates whether the delta stream currently has a wildcard watch
	wildcard bool

	// Provides the list of resources explicitly requested by the client
	// This list might be non-empty even when set as wildcard
	subscribedResourceNames map[string]struct{}

	// type url to state
	subscriedSotwResources map[string]*SotwSubscribed

	// ResourceVersions contains a hash of the resource as the value and the resource name as the key.
	// This field stores the last state sent to the client.
	resourceVersions map[string]string

	// knownResourceNames contains resource names that a client has received previously
	knownResourceNames map[string]map[string]struct{}

	// indicates whether the object has been modified since its creation
	first bool
}

// GetSubscribedResourceNames returns the list of resources currently explicitly subscribed to
// If the request is set to wildcard it may be empty
// Currently populated only when using delta-xds
func (s *StreamState) GetSubscribedResourceNames() map[string]struct{} {
	return s.subscribedResourceNames
}

// SetSubscribedResourceNames is setting the list of resources currently explicitly subscribed to
// It is decorrelated from the wildcard state of the stream
// Currently used only when using delta-xds
func (s *StreamState) SetSubscribedResourceNames(subscribedResourceNames map[string]struct{}) {
	s.subscribedResourceNames = subscribedResourceNames
}

// WatchesResources returns whether at least one of the resource provided is currently watch by the stream
// It is currently only applicable to delta-xds
// If the request is wildcard, it will always return true
// Otherwise it will compare the provided resources to the list of resources currently subscribed
func (s *StreamState) WatchesResources(resourceNames map[string]struct{}) bool {
	if s.IsWildcard() {
		return true
	}
	for resourceName := range resourceNames {
		if _, ok := s.subscribedResourceNames[resourceName]; ok {
			return true
		}
	}
	return false
}

// SetSubscribedSotwResources is setting the list of resources currently explicitly subscribed to
func (s *StreamState) SetSubscribedSotwResources(url string, names []string) {
	if _, found := s.subscriedSotwResources[url]; found {
		newResources := &SotwSubscribed{
			subscribedResourceNames: make(map[string]struct{}),
		}
		newResources.setNames(names)
		s.subscriedSotwResources[url] = newResources
	} else {
		// At first, when names are unset, it's *.
		// Here wew use not found to present not setting, which means *.
		// Notice, LDS, CDS are always empty, and we need to keep this if all requests are unset until it's set.
		// https://www.envoyproxy.io/docs/envoy/v1.24.0/api-docs/xds_protocol#how-the-client-specifies-what-resources-to-return
		if len(names) == 0 {
			return
		}
		newResources := &SotwSubscribed{
			subscribedResourceNames: make(map[string]struct{}),
		}
		newResources.setNames(names)
		s.subscriedSotwResources[url] = newResources
	}
}

// WatchesSotwAll returns whether currently watch all resources. If not, request.ResourceNames are watching resources.
func (s *StreamState) WatchesSotwAll(url string) bool {
	resources, found := s.subscriedSotwResources[url]
	// We don't set it when it's always unset. See SetSubscribedSotwResources
	if !found {
		return true
	}
	if resources.wildcard {
		return true
	}
	return false
}

// WatchesSotwAll returns whether newly watch all resources. If not, request.ResourceNames are watching resources.
func (s *StreamState) WatchesSotwAllNew(url string, names []string) bool {
	if _, found := s.subscriedSotwResources[url]; !found {
		// Here wew use not found to present not setting, which means *.
		if len(names) == 0 {
			return true
		}
	}
	newResources := &SotwSubscribed{
		subscribedResourceNames: make(map[string]struct{}),
	}
	newResources.setNames(names)
	return newResources.wildcard
}

// WatchesSotwAll returns whether newly watch all resources. If not, request.ResourceNames are watching resources.
func (s *StreamState) WatchesSotwResource(url string, name string) bool {
	resources, found := s.subscriedSotwResources[url]
	if !found {
		// We don't set it when it's always unset. See SetSubscribedSotwResources
		return true
	}
	if resources.wildcard {
		return true
	}
	if _, ok := resources.subscribedResourceNames[name]; ok {
		return true
	}
	return false
}

func (s *StreamState) GetResourceVersions() map[string]string {
	return s.resourceVersions
}

func (s *StreamState) SetResourceVersions(resourceVersions map[string]string) {
	s.first = false
	s.resourceVersions = resourceVersions
}

func (s *StreamState) IsFirst() bool {
	return s.first
}

func (s *StreamState) SetWildcard(wildcard bool) {
	s.wildcard = wildcard
}

func (s *StreamState) IsWildcard() bool {
	return s.wildcard
}

func (s *StreamState) SetKnownResourceNames(url string, names map[string]struct{}) {
	s.knownResourceNames[url] = names
}

func (s *StreamState) SetKnownResourceNamesAsList(url string, names []string) {
	m := map[string]struct{}{}
	for _, name := range names {
		m[name] = struct{}{}
	}
	s.knownResourceNames[url] = m
}

func (s *StreamState) GetKnownResourceNames(url string) map[string]struct{} {
	return s.knownResourceNames[url]
}

// NewStreamState initializes a stream state.
func NewStreamState(wildcard bool, initialResourceVersions map[string]string) StreamState {
	state := StreamState{
		wildcard:                wildcard,
		subscribedResourceNames: map[string]struct{}{},
		subscriedSotwResources:  map[string]*SotwSubscribed{},
		resourceVersions:        initialResourceVersions,
		first:                   true,
		knownResourceNames:      map[string]map[string]struct{}{},
	}

	if initialResourceVersions == nil {
		state.resourceVersions = make(map[string]string)
	}

	return state
}

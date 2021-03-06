package services

import (
	"context"

	"github.com/deps-cloud/api"
	"github.com/deps-cloud/api/v1alpha/schema"
	"github.com/deps-cloud/api/v1alpha/store"
	"github.com/deps-cloud/api/v1alpha/tracker"
	"github.com/deps-cloud/tracker/pkg/types"

	"google.golang.org/grpc"
)

// RegisterDependencyService registers the dependencyService implementation with the server
func RegisterDependencyService(server *grpc.Server, gs store.GraphStoreClient) {
	tracker.RegisterDependencyServiceServer(server, &dependencyService{gs: gs})
}

type dependencyService struct {
	gs store.GraphStoreClient
}

var _ tracker.DependencyServiceServer = &dependencyService{}

func keyForDependencyRequest(req *tracker.DependencyRequest) []byte {
	return keyForModule(&schema.Module{
		Language:     req.GetLanguage(),
		Organization: req.GetOrganization(),
		Module:       req.GetModule(),
	})
}

func (d *dependencyService) ListDependents(ctx context.Context, req *tracker.DependencyRequest) (*tracker.ListDependentsResponse, error) {
	key := keyForDependencyRequest(req)

	response, err := d.gs.FindDownstream(ctx, &store.FindRequest{
		Key:       key,
		EdgeTypes: []string{types.DependsType},
	})

	if err != nil {
		return nil, api.ErrModuleNotFound
	}

	dependents := make([]*tracker.Dependency, len(response.GetPairs()))
	for i, pair := range response.GetPairs() {
		a, _ := Decode(pair.Node)
		b, _ := Decode(pair.Edge)

		dependents[i] = &tracker.Dependency{
			Module:  a.(*schema.Module),
			Depends: b.(*schema.Depends),
		}
	}

	return &tracker.ListDependentsResponse{
		Dependents: dependents,
	}, nil
}

func (d *dependencyService) ListDependencies(ctx context.Context, req *tracker.DependencyRequest) (*tracker.ListDependenciesResponse, error) {
	key := keyForDependencyRequest(req)

	response, err := d.gs.FindUpstream(ctx, &store.FindRequest{
		Key:       key,
		EdgeTypes: []string{types.DependsType},
	})

	if err != nil {
		return nil, api.ErrModuleNotFound
	}

	dependencies := make([]*tracker.Dependency, len(response.GetPairs()))
	for i, pair := range response.GetPairs() {
		a, _ := Decode(pair.Node)
		b, _ := Decode(pair.Edge)

		dependencies[i] = &tracker.Dependency{
			Module:  a.(*schema.Module),
			Depends: b.(*schema.Depends),
		}
	}

	return &tracker.ListDependenciesResponse{
		Dependencies: dependencies,
	}, nil
}

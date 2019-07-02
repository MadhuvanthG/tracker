// Code generated by protoc-gen-grpc-gateway. DO NOT EDIT.
// source: service.proto

/*
Package v1alpha is a reverse proxy.

It translates gRPC into RESTful JSON APIs.
*/
package v1alpha

import (
	"context"
	"io"
	"net/http"

	"github.com/deps-cloud/tracker/api/v1alpha/schema"
	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/grpc-ecosystem/grpc-gateway/utilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
)

var _ codes.Code
var _ io.Reader
var _ status.Status
var _ = runtime.String
var _ = utilities.NewDoubleArray

var (
	filter_SourceService_List_0 = &utilities.DoubleArray{Encoding: map[string]int{}, Base: []int(nil), Check: []int(nil)}
)

func request_SourceService_List_0(ctx context.Context, marshaler runtime.Marshaler, client SourceServiceClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var protoReq ListRequest
	var metadata runtime.ServerMetadata

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_SourceService_List_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	msg, err := client.List(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err

}

func request_SourceService_Track_0(ctx context.Context, marshaler runtime.Marshaler, client SourceServiceClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var protoReq SourceRequest
	var metadata runtime.ServerMetadata

	newReader, berr := utilities.IOReaderFactory(req.Body)
	if berr != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", berr)
	}
	if err := marshaler.NewDecoder(newReader()).Decode(&protoReq); err != nil && err != io.EOF {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	msg, err := client.Track(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err

}

var (
	filter_ModuleService_List_0 = &utilities.DoubleArray{Encoding: map[string]int{}, Base: []int(nil), Check: []int(nil)}
)

func request_ModuleService_List_0(ctx context.Context, marshaler runtime.Marshaler, client ModuleServiceClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var protoReq ListRequest
	var metadata runtime.ServerMetadata

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_ModuleService_List_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	msg, err := client.List(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))
	return msg, metadata, err

}

var (
	filter_ModuleService_GetSource_0 = &utilities.DoubleArray{Encoding: map[string]int{}, Base: []int(nil), Check: []int(nil)}
)

func request_ModuleService_GetSource_0(ctx context.Context, marshaler runtime.Marshaler, client ModuleServiceClient, req *http.Request, pathParams map[string]string) (ModuleService_GetSourceClient, runtime.ServerMetadata, error) {
	var protoReq schema.Module
	var metadata runtime.ServerMetadata

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_ModuleService_GetSource_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetSource(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

var (
	filter_ModuleService_GetManaged_0 = &utilities.DoubleArray{Encoding: map[string]int{}, Base: []int(nil), Check: []int(nil)}
)

func request_ModuleService_GetManaged_0(ctx context.Context, marshaler runtime.Marshaler, client ModuleServiceClient, req *http.Request, pathParams map[string]string) (ModuleService_GetManagedClient, runtime.ServerMetadata, error) {
	var protoReq schema.Source
	var metadata runtime.ServerMetadata

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_ModuleService_GetManaged_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetManaged(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

var (
	filter_DependencyService_GetDependents_0 = &utilities.DoubleArray{Encoding: map[string]int{"language": 0}, Base: []int{1, 1, 0}, Check: []int{0, 1, 2}}
)

func request_DependencyService_GetDependents_0(ctx context.Context, marshaler runtime.Marshaler, client DependencyServiceClient, req *http.Request, pathParams map[string]string) (DependencyService_GetDependentsClient, runtime.ServerMetadata, error) {
	var protoReq DependencyRequest
	var metadata runtime.ServerMetadata

	var (
		val string
		ok  bool
		err error
		_   = err
	)

	val, ok = pathParams["language"]
	if !ok {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "missing parameter %s", "language")
	}

	protoReq.Language, err = runtime.String(val)

	if err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "type mismatch, parameter: %s, error: %v", "language", err)
	}

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_DependencyService_GetDependents_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetDependents(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

var (
	filter_DependencyService_GetDependencies_0 = &utilities.DoubleArray{Encoding: map[string]int{"language": 0}, Base: []int{1, 1, 0}, Check: []int{0, 1, 2}}
)

func request_DependencyService_GetDependencies_0(ctx context.Context, marshaler runtime.Marshaler, client DependencyServiceClient, req *http.Request, pathParams map[string]string) (DependencyService_GetDependenciesClient, runtime.ServerMetadata, error) {
	var protoReq DependencyRequest
	var metadata runtime.ServerMetadata

	var (
		val string
		ok  bool
		err error
		_   = err
	)

	val, ok = pathParams["language"]
	if !ok {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "missing parameter %s", "language")
	}

	protoReq.Language, err = runtime.String(val)

	if err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "type mismatch, parameter: %s, error: %v", "language", err)
	}

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_DependencyService_GetDependencies_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetDependencies(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

var (
	filter_TopologyService_GetDependentsTopology_0 = &utilities.DoubleArray{Encoding: map[string]int{"language": 0}, Base: []int{1, 1, 0}, Check: []int{0, 1, 2}}
)

func request_TopologyService_GetDependentsTopology_0(ctx context.Context, marshaler runtime.Marshaler, client TopologyServiceClient, req *http.Request, pathParams map[string]string) (TopologyService_GetDependentsTopologyClient, runtime.ServerMetadata, error) {
	var protoReq DependencyRequest
	var metadata runtime.ServerMetadata

	var (
		val string
		ok  bool
		err error
		_   = err
	)

	val, ok = pathParams["language"]
	if !ok {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "missing parameter %s", "language")
	}

	protoReq.Language, err = runtime.String(val)

	if err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "type mismatch, parameter: %s, error: %v", "language", err)
	}

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_TopologyService_GetDependentsTopology_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetDependentsTopology(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

var (
	filter_TopologyService_GetDependentsTopologyTiered_0 = &utilities.DoubleArray{Encoding: map[string]int{"language": 0}, Base: []int{1, 1, 0}, Check: []int{0, 1, 2}}
)

func request_TopologyService_GetDependentsTopologyTiered_0(ctx context.Context, marshaler runtime.Marshaler, client TopologyServiceClient, req *http.Request, pathParams map[string]string) (TopologyService_GetDependentsTopologyTieredClient, runtime.ServerMetadata, error) {
	var protoReq DependencyRequest
	var metadata runtime.ServerMetadata

	var (
		val string
		ok  bool
		err error
		_   = err
	)

	val, ok = pathParams["language"]
	if !ok {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "missing parameter %s", "language")
	}

	protoReq.Language, err = runtime.String(val)

	if err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "type mismatch, parameter: %s, error: %v", "language", err)
	}

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_TopologyService_GetDependentsTopologyTiered_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetDependentsTopologyTiered(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

var (
	filter_TopologyService_GetDependenciesTopology_0 = &utilities.DoubleArray{Encoding: map[string]int{"language": 0}, Base: []int{1, 1, 0}, Check: []int{0, 1, 2}}
)

func request_TopologyService_GetDependenciesTopology_0(ctx context.Context, marshaler runtime.Marshaler, client TopologyServiceClient, req *http.Request, pathParams map[string]string) (TopologyService_GetDependenciesTopologyClient, runtime.ServerMetadata, error) {
	var protoReq DependencyRequest
	var metadata runtime.ServerMetadata

	var (
		val string
		ok  bool
		err error
		_   = err
	)

	val, ok = pathParams["language"]
	if !ok {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "missing parameter %s", "language")
	}

	protoReq.Language, err = runtime.String(val)

	if err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "type mismatch, parameter: %s, error: %v", "language", err)
	}

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_TopologyService_GetDependenciesTopology_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetDependenciesTopology(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

var (
	filter_TopologyService_GetDependenciesTopologyTiered_0 = &utilities.DoubleArray{Encoding: map[string]int{"language": 0}, Base: []int{1, 1, 0}, Check: []int{0, 1, 2}}
)

func request_TopologyService_GetDependenciesTopologyTiered_0(ctx context.Context, marshaler runtime.Marshaler, client TopologyServiceClient, req *http.Request, pathParams map[string]string) (TopologyService_GetDependenciesTopologyTieredClient, runtime.ServerMetadata, error) {
	var protoReq DependencyRequest
	var metadata runtime.ServerMetadata

	var (
		val string
		ok  bool
		err error
		_   = err
	)

	val, ok = pathParams["language"]
	if !ok {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "missing parameter %s", "language")
	}

	protoReq.Language, err = runtime.String(val)

	if err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "type mismatch, parameter: %s, error: %v", "language", err)
	}

	if err := req.ParseForm(); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if err := runtime.PopulateQueryParameters(&protoReq, req.Form, filter_TopologyService_GetDependenciesTopologyTiered_0); err != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	stream, err := client.GetDependenciesTopologyTiered(ctx, &protoReq)
	if err != nil {
		return nil, metadata, err
	}
	header, err := stream.Header()
	if err != nil {
		return nil, metadata, err
	}
	metadata.HeaderMD = header
	return stream, metadata, nil

}

// RegisterSourceServiceHandlerFromEndpoint is same as RegisterSourceServiceHandler but
// automatically dials to "endpoint" and closes the connection when "ctx" gets done.
func RegisterSourceServiceHandlerFromEndpoint(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error) {
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
		}()
	}()

	return RegisterSourceServiceHandler(ctx, mux, conn)
}

// RegisterSourceServiceHandler registers the http handlers for service SourceService to "mux".
// The handlers forward requests to the grpc endpoint over "conn".
func RegisterSourceServiceHandler(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	return RegisterSourceServiceHandlerClient(ctx, mux, NewSourceServiceClient(conn))
}

// RegisterSourceServiceHandlerClient registers the http handlers for service SourceService
// to "mux". The handlers forward requests to the grpc endpoint over the given implementation of "SourceServiceClient".
// Note: the gRPC framework executes interceptors within the gRPC handler. If the passed in "SourceServiceClient"
// doesn't go through the normal gRPC flow (creating a gRPC client etc.) then it will be up to the passed in
// "SourceServiceClient" to call the correct interceptors.
func RegisterSourceServiceHandlerClient(ctx context.Context, mux *runtime.ServeMux, client SourceServiceClient) error {

	mux.Handle("GET", pattern_SourceService_List_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_SourceService_List_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_SourceService_List_0(ctx, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)

	})

	mux.Handle("POST", pattern_SourceService_Track_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_SourceService_Track_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_SourceService_Track_0(ctx, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)

	})

	return nil
}

var (
	pattern_SourceService_List_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"v1alpha", "sources"}, ""))

	pattern_SourceService_Track_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 2, 2}, []string{"v1alpha", "sources", "track"}, ""))
)

var (
	forward_SourceService_List_0 = runtime.ForwardResponseMessage

	forward_SourceService_Track_0 = runtime.ForwardResponseMessage
)

// RegisterModuleServiceHandlerFromEndpoint is same as RegisterModuleServiceHandler but
// automatically dials to "endpoint" and closes the connection when "ctx" gets done.
func RegisterModuleServiceHandlerFromEndpoint(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error) {
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
		}()
	}()

	return RegisterModuleServiceHandler(ctx, mux, conn)
}

// RegisterModuleServiceHandler registers the http handlers for service ModuleService to "mux".
// The handlers forward requests to the grpc endpoint over "conn".
func RegisterModuleServiceHandler(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	return RegisterModuleServiceHandlerClient(ctx, mux, NewModuleServiceClient(conn))
}

// RegisterModuleServiceHandlerClient registers the http handlers for service ModuleService
// to "mux". The handlers forward requests to the grpc endpoint over the given implementation of "ModuleServiceClient".
// Note: the gRPC framework executes interceptors within the gRPC handler. If the passed in "ModuleServiceClient"
// doesn't go through the normal gRPC flow (creating a gRPC client etc.) then it will be up to the passed in
// "ModuleServiceClient" to call the correct interceptors.
func RegisterModuleServiceHandlerClient(ctx context.Context, mux *runtime.ServeMux, client ModuleServiceClient) error {

	mux.Handle("GET", pattern_ModuleService_List_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_ModuleService_List_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_ModuleService_List_0(ctx, mux, outboundMarshaler, w, req, resp, mux.GetForwardResponseOptions()...)

	})

	mux.Handle("GET", pattern_ModuleService_GetSource_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_ModuleService_GetSource_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_ModuleService_GetSource_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	mux.Handle("GET", pattern_ModuleService_GetManaged_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_ModuleService_GetManaged_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_ModuleService_GetManaged_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	return nil
}

var (
	pattern_ModuleService_List_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1}, []string{"v1alpha", "modules"}, ""))

	pattern_ModuleService_GetSource_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 2, 2}, []string{"v1alpha", "modules", "source"}, ""))

	pattern_ModuleService_GetManaged_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 2, 2}, []string{"v1alpha", "modules", "managed"}, ""))
)

var (
	forward_ModuleService_List_0 = runtime.ForwardResponseMessage

	forward_ModuleService_GetSource_0 = runtime.ForwardResponseStream

	forward_ModuleService_GetManaged_0 = runtime.ForwardResponseStream
)

// RegisterDependencyServiceHandlerFromEndpoint is same as RegisterDependencyServiceHandler but
// automatically dials to "endpoint" and closes the connection when "ctx" gets done.
func RegisterDependencyServiceHandlerFromEndpoint(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error) {
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
		}()
	}()

	return RegisterDependencyServiceHandler(ctx, mux, conn)
}

// RegisterDependencyServiceHandler registers the http handlers for service DependencyService to "mux".
// The handlers forward requests to the grpc endpoint over "conn".
func RegisterDependencyServiceHandler(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	return RegisterDependencyServiceHandlerClient(ctx, mux, NewDependencyServiceClient(conn))
}

// RegisterDependencyServiceHandlerClient registers the http handlers for service DependencyService
// to "mux". The handlers forward requests to the grpc endpoint over the given implementation of "DependencyServiceClient".
// Note: the gRPC framework executes interceptors within the gRPC handler. If the passed in "DependencyServiceClient"
// doesn't go through the normal gRPC flow (creating a gRPC client etc.) then it will be up to the passed in
// "DependencyServiceClient" to call the correct interceptors.
func RegisterDependencyServiceHandlerClient(ctx context.Context, mux *runtime.ServeMux, client DependencyServiceClient) error {

	mux.Handle("GET", pattern_DependencyService_GetDependents_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_DependencyService_GetDependents_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_DependencyService_GetDependents_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	mux.Handle("GET", pattern_DependencyService_GetDependencies_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_DependencyService_GetDependencies_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_DependencyService_GetDependencies_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	return nil
}

var (
	pattern_DependencyService_GetDependents_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 1, 0, 4, 1, 5, 2, 2, 3}, []string{"v1alpha", "graph", "language", "dependents"}, ""))

	pattern_DependencyService_GetDependencies_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 1, 0, 4, 1, 5, 2, 2, 3}, []string{"v1alpha", "graph", "language", "dependencies"}, ""))
)

var (
	forward_DependencyService_GetDependents_0 = runtime.ForwardResponseStream

	forward_DependencyService_GetDependencies_0 = runtime.ForwardResponseStream
)

// RegisterTopologyServiceHandlerFromEndpoint is same as RegisterTopologyServiceHandler but
// automatically dials to "endpoint" and closes the connection when "ctx" gets done.
func RegisterTopologyServiceHandlerFromEndpoint(ctx context.Context, mux *runtime.ServeMux, endpoint string, opts []grpc.DialOption) (err error) {
	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
			return
		}
		go func() {
			<-ctx.Done()
			if cerr := conn.Close(); cerr != nil {
				grpclog.Infof("Failed to close conn to %s: %v", endpoint, cerr)
			}
		}()
	}()

	return RegisterTopologyServiceHandler(ctx, mux, conn)
}

// RegisterTopologyServiceHandler registers the http handlers for service TopologyService to "mux".
// The handlers forward requests to the grpc endpoint over "conn".
func RegisterTopologyServiceHandler(ctx context.Context, mux *runtime.ServeMux, conn *grpc.ClientConn) error {
	return RegisterTopologyServiceHandlerClient(ctx, mux, NewTopologyServiceClient(conn))
}

// RegisterTopologyServiceHandlerClient registers the http handlers for service TopologyService
// to "mux". The handlers forward requests to the grpc endpoint over the given implementation of "TopologyServiceClient".
// Note: the gRPC framework executes interceptors within the gRPC handler. If the passed in "TopologyServiceClient"
// doesn't go through the normal gRPC flow (creating a gRPC client etc.) then it will be up to the passed in
// "TopologyServiceClient" to call the correct interceptors.
func RegisterTopologyServiceHandlerClient(ctx context.Context, mux *runtime.ServeMux, client TopologyServiceClient) error {

	mux.Handle("GET", pattern_TopologyService_GetDependentsTopology_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_TopologyService_GetDependentsTopology_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_TopologyService_GetDependentsTopology_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	mux.Handle("GET", pattern_TopologyService_GetDependentsTopologyTiered_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_TopologyService_GetDependentsTopologyTiered_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_TopologyService_GetDependentsTopologyTiered_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	mux.Handle("GET", pattern_TopologyService_GetDependenciesTopology_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_TopologyService_GetDependenciesTopology_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_TopologyService_GetDependenciesTopology_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	mux.Handle("GET", pattern_TopologyService_GetDependenciesTopologyTiered_0, func(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(mux, req)
		rctx, err := runtime.AnnotateContext(ctx, mux, req)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}
		resp, md, err := request_TopologyService_GetDependenciesTopologyTiered_0(rctx, inboundMarshaler, client, req, pathParams)
		ctx = runtime.NewServerMetadataContext(ctx, md)
		if err != nil {
			runtime.HTTPError(ctx, mux, outboundMarshaler, w, req, err)
			return
		}

		forward_TopologyService_GetDependenciesTopologyTiered_0(ctx, mux, outboundMarshaler, w, req, func() (proto.Message, error) { return resp.Recv() }, mux.GetForwardResponseOptions()...)

	})

	return nil
}

var (
	pattern_TopologyService_GetDependentsTopology_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 1, 0, 4, 1, 5, 2, 2, 3, 2, 4}, []string{"v1alpha", "graph", "language", "dependents", "topology"}, ""))

	pattern_TopologyService_GetDependentsTopologyTiered_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 1, 0, 4, 1, 5, 2, 2, 3, 2, 4, 2, 5}, []string{"v1alpha", "graph", "language", "dependents", "topology", "tiered"}, ""))

	pattern_TopologyService_GetDependenciesTopology_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 1, 0, 4, 1, 5, 2, 2, 3, 2, 4}, []string{"v1alpha", "graph", "language", "dependencies", "topology"}, ""))

	pattern_TopologyService_GetDependenciesTopologyTiered_0 = runtime.MustPattern(runtime.NewPattern(1, []int{2, 0, 2, 1, 1, 0, 4, 1, 5, 2, 2, 3, 2, 4, 2, 5}, []string{"v1alpha", "graph", "language", "dependencies", "topology", "tiered"}, ""))
)

var (
	forward_TopologyService_GetDependentsTopology_0 = runtime.ForwardResponseStream

	forward_TopologyService_GetDependentsTopologyTiered_0 = runtime.ForwardResponseStream

	forward_TopologyService_GetDependenciesTopology_0 = runtime.ForwardResponseStream

	forward_TopologyService_GetDependenciesTopologyTiered_0 = runtime.ForwardResponseStream
)

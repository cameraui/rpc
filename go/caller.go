package rpc

import "context"

// CallerRoleHeader is the header the camera.ui proxy sets on every message of a viewer connection.
const CallerRoleHeader = "Cui-Role"

const viewerRole = "viewer"

type callerKey struct{}

// CallerIsViewer reports whether the call behind ctx came from a viewer account.
func CallerIsViewer(ctx context.Context) bool {
	viewer, _ := ctx.Value(callerKey{}).(bool)
	return viewer
}

func withCaller(ctx context.Context, viewer bool) context.Context {
	if !viewer {
		return ctx
	}
	return context.WithValue(ctx, callerKey{}, true)
}

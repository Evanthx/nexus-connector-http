package httpconnector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/nexus-connector-http/generated/httpapi"
	"go.temporal.io/sdk/temporal"
)

// Send needs no activity: a synchronous Nexus handler or a library caller runs it with a plain
// context, and it gives the same response and typed errors as Request.
func TestSend_WithoutActivity(t *testing.T) {
	a := newWithWindow(testWindow)
	ok, _ := countingServer(t, http.StatusOK, "fine")
	resp, err := a.Send(context.Background(), httpapi.RequestInput{URL: ok.URL})
	require.NoError(t, err)
	require.Equal(t, "fine", resp.Body)

	missing, _ := countingServer(t, http.StatusNotFound, "no such thing")
	_, err = a.Send(context.Background(), httpapi.RequestInput{URL: missing.URL})
	require.Equal(t, ErrorTypeClientError, appErr(t, err).Type())

	limited, _ := countingServer(t, http.StatusTooManyRequests, "")
	_, err = a.Send(context.Background(), httpapi.RequestInput{URL: limited.URL})
	require.Equal(t, ErrorTypeRateLimited, appErr(t, err).Type())
}

// HandlerError keeps the ApplicationError as the cause, so errors.As finds its type and details as
// in activity mode, and gives each type the handler error whose retry behaviour matches it.
func TestHandlerError(t *testing.T) {
	out := httpapi.RequestOutput{StatusCode: 404, Body: "no such thing"}
	for _, tc := range []struct {
		err       error
		wantType  nexus.HandlerErrorType
		retryable bool
	}{
		{InvalidRequestError(errors.New("bad url")), nexus.HandlerErrorTypeBadRequest, false},
		{temporal.NewApplicationErrorWithOptions("404", ErrorTypeClientError, temporal.ApplicationErrorOptions{NonRetryable: true, Details: []any{out}}),
			nexus.HandlerErrorTypeInternal, false},
		{rateLimitedError("429", time.Minute, nil), nexus.HandlerErrorTypeResourceExhausted, true},
		{transientError("503", nil), nexus.HandlerErrorTypeUnavailable, true},
		{errors.New("unexpected"), nexus.HandlerErrorTypeInternal, true},
	} {
		var he *nexus.HandlerError
		require.ErrorAs(t, HandlerError(tc.err), &he)
		require.Equal(t, tc.wantType, he.Type, tc.err.Error())
		require.Equal(t, tc.retryable, he.Retryable(), tc.err.Error())
		var ae *temporal.ApplicationError
		if errors.As(tc.err, &ae) {
			require.Equal(t, ae.Message(), he.Message, "the message names the problem")
			require.Same(t, ae, appErr(t, he))
		} else {
			require.Equal(t, tc.err.Error(), he.Message, "the message names the problem")
		}
	}
}

// A call the endpoint doesn't answer in time stops before the Nexus request's deadline, so the
// handler can still reply, with a retryable error that says to use activity mode.
func TestRequestSync_StopsBeforeTheRequestDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	a := newWithWindow(testWindow)
	ctx, cancel := context.WithTimeout(context.Background(), replyMargin+time.Second)
	defer cancel()
	start := time.Now()
	_, err := a.RequestSync(ctx, httpapi.RequestInput{URL: srv.URL})
	require.NoError(t, ctx.Err(), "replied before the request's deadline")
	require.Less(t, time.Since(start), replyMargin+time.Second)
	var he *nexus.HandlerError
	require.ErrorAs(t, err, &he)
	require.Equal(t, nexus.HandlerErrorTypeUnavailable, he.Type)
	require.True(t, he.Retryable())
	require.Equal(t, ErrorTypeTransient, appErr(t, err).Type())
	require.Contains(t, he.Message, "no response before the Nexus request's deadline")
	require.Contains(t, he.Message, ModeEnvVar+"=activity")
}

func TestParseMode(t *testing.T) {
	m, err := ParseMode("")
	require.NoError(t, err)
	require.Equal(t, ModeActivity, m)
	require.Equal(t, ModeActivity, New(Config{}, nil).Mode(), "New defaults to activity")
	_, err = ParseMode("async")
	require.ErrorContains(t, err, ModeEnvVar)
}

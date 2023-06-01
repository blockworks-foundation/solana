use std::{
    error::Error,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower::{Layer, Service};

/// Action undertaken by a middleware.
pub enum RequestMiddlewareAction {
    /// Proceed with standard RPC handling
    Proceed,
    /// Intercept the request and respond differently.
    Respond(Pin<Box<dyn Future<Output = hyper::Result<hyper::Response<hyper::Body>>> + Send>>),
}

impl From<hyper::Response<hyper::Body>> for RequestMiddlewareAction {
    fn from(value: hyper::Response<hyper::Body>) -> Self {
        Self::Respond(Box::pin(async move { Ok(value) }))
    }
}

/// Allows to intercept request and handle it differently.
pub trait RequestMiddleware: Send + Sync + 'static {
    /// Takes a request and decides how to proceed with it.
    fn on_request(&self, request: &hyper::Request<hyper::Body>) -> RequestMiddlewareAction;
}

/// Layer that applies [`ProxyGetRequest`] which proxies the `GET /path` requests to
/// specific RPC method calls and that strips the response.
///
/// See [`ProxyGetRequest`] for more details.
#[derive(Clone)]
pub struct ProxyMiddleware<R: RequestMiddleware>(Arc<R>);

impl<R: RequestMiddleware> ProxyMiddleware<R> {
    pub fn new(req_middleware: R) -> Self {
        Self(Arc::new(req_middleware))
    }
}

impl<S, R: RequestMiddleware> Layer<S> for ProxyMiddleware<R> {
    type Service = ProxyGetRequest<S, R>;

    fn layer(&self, inner: S) -> Self::Service {
        ProxyGetRequest {
            inner,
            req_middleware: self.0.clone(),
        }
    }
}

/// Proxy requests to the middleware and switch according to [`RequestMiddlewareAction`]
#[derive(Clone)]
pub struct ProxyGetRequest<S, R: RequestMiddleware> {
    inner: S,
    req_middleware: Arc<R>,
}

impl<S, R: RequestMiddleware> Service<hyper::Request<hyper::Body>> for ProxyGetRequest<S, R>
where
    S: Service<hyper::Request<hyper::Body>, Response = hyper::Response<hyper::Body>>,
    S::Response: 'static,
    S::Error: Into<Box<dyn Error + Send + Sync>> + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = Box<dyn Error + Send + Sync + 'static>;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: hyper::Request<hyper::Body>) -> Self::Future {
        let RequestMiddlewareAction::Respond(res) = self.req_middleware.on_request(&req) else {
            let fut = self.inner.call(req);
            let fut = async move {
                fut.await.map_err(|err| err.into())
            };

            return Box::pin(fut);

        };

        Box::pin(async { res.await.map_err(|err| err.into()) })
    }
}

use std::future::Future;
use std::pin::Pin;

use hyper::Body;

/// Action undertaken by a middleware.
pub enum RequestMiddlewareAction {
    /// Proceed with standard RPC handling
    Proceed {
        /// Should the request be processed even if invalid CORS headers are detected?
        /// This allows for side effects to take place.
        should_continue_on_invalid_cors: bool,
        /// The request object returned
        request: hyper::Request<Body>,
    },
    /// Intercept the request and respond differently.
    Respond {
        /// Should standard hosts validation be performed?
        should_validate_hosts: bool,
        /// a future for server response
        response: Pin<Box<dyn Future<Output = hyper::Result<hyper::Response<Body>>> + Send>>,
    },
}

//impl From<Response> for RequestMiddlewareAction {
//    fn from(o: Response) -> Self {
//        RequestMiddlewareAction::Respond {
//            should_validate_hosts: true,
//            response: Box::pin(async { Ok(o.into()) }),
//        }
//    }
//}

impl From<hyper::Response<Body>> for RequestMiddlewareAction {
    fn from(response: hyper::Response<Body>) -> Self {
        RequestMiddlewareAction::Respond {
            should_validate_hosts: true,
            response: Box::pin(async { Ok(response) }),
        }
    }
}

impl From<hyper::Request<Body>> for RequestMiddlewareAction {
    fn from(request: hyper::Request<Body>) -> Self {
        RequestMiddlewareAction::Proceed {
            should_continue_on_invalid_cors: false,
            request,
        }
    }
}

/// Allows to intercept request and handle it differently.
pub trait RequestMiddleware: Send + Sync + 'static {
    /// Takes a request and decides how to proceed with it.
    fn on_request(&self, request: hyper::Request<hyper::Body>) -> RequestMiddlewareAction;
}

impl<F> RequestMiddleware for F
where
    F: Fn(hyper::Request<Body>) -> RequestMiddlewareAction + Sync + Send + 'static,
{
    fn on_request(&self, request: hyper::Request<hyper::Body>) -> RequestMiddlewareAction {
        (*self)(request)
    }
}

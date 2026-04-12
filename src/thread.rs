use crate::event_loop::EventLoopHandle;

pub fn spawn<F>(fut: F)
where
    F: Future<Output = ()> + 'static,
{
    EventLoopHandle::current()
        .expect("no active runtime")
        .spawn(fut);
}

mod poll;
mod reactor;
mod sheduled;

pub(crate) const READABLE: usize = 0b01;
pub(crate) const WRITABLE: usize = 0b10;

pub use poll::PollEvented;
pub use reactor::IoReactor;
pub use sheduled::ScheduledIo;

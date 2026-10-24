/* The io_uring build's view of the kernel headers. Asio enables io_uring only
   when the headers it is built with are Linux 5.10 or later, but the build
   images carry older ones (manylinux_2_28: 4.18; Ubuntu 20.04: 5.4) and the
   io_uring definitions come from liburing's own headers. Whether the running
   kernel can do it is checked when the runtime starts, which falls back to
   epoll. */
#define LINUX_VERSION_CODE 330240
#define KERNEL_VERSION(a,b,c) (((a) << 16) + ((b) << 8) + ((c) > 255 ? 255 : (c)))
#define LINUX_VERSION_MAJOR 5
#define LINUX_VERSION_PATCHLEVEL 10
#define LINUX_VERSION_SUBLEVEL 0

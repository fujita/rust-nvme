# Rust simple NVMe device driver

This is for figuring out proper Rust PCI, DMA, block layer abstraction APIs.
    
It can handle some I/Os with QEMU. But no proper error handling or resource cleanup.

[Rust-for-Linux tree](https://github.com/Rust-for-Linux/linux) doesn't support abstraction APIs (PCI, DMA, block layer, etc) yet. This driver is tested with [my fork](https://github.com/fujita/linux/rust-nvme). I'll work for upstreaming.

```bash
$ make KDIR=~/git/linux LLVM=1
```

// SPDX-License-Identifier: GPL-2.0

// ! Rust simple nvme driver

use core::ffi::c_void;
use core::{marker::PhantomPinned, mem::zeroed, pin::Pin, ptr};
use kernel::bindings;
use kernel::driver;
use kernel::irq;
use kernel::pcidev;
use kernel::prelude::*;
use kernel::sync::{Ref, RefBorrow, SpinLock};
use kernel::Result;
use kernel::{c_str, str::CStr};

struct BlkDevOps;

impl BlkDevOps {
    const OPS: bindings::block_device_operations = bindings::block_device_operations {
        submit_bio: None,
        poll_bio: None,
        open: None,
        release: None,
        rw_page: None,
        ioctl: None,
        compat_ioctl: None,
        check_events: None,
        unlock_native_capacity: None,
        getgeo: None,
        set_read_only: None,
        free_disk: None,
        swap_slot_free_notify: None,
        report_zones: None,
        devnode: None,
        get_unique_id: None,
        owner: ptr::null_mut(),
        pr_ops: ptr::null_mut(),
        alternative_gpt_sector: None,
    };
}

struct Adapter<T: BlkRqOps>(T);

impl<T: BlkRqOps> Adapter<T> {
    unsafe extern "C" fn init_request(
        set: *mut bindings::blk_mq_tag_set,
        rq: *mut bindings::request,
        arg2: core::ffi::c_uint,
        arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int {
        T::init_request(set, rq, arg2, arg3)
    }
    unsafe extern "C" fn init_hctx(
        arg1: *mut bindings::blk_mq_hw_ctx,
        arg2: *mut core::ffi::c_void,
        arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int {
        T::init_hctx(arg1, arg2, arg3)
    }

    unsafe extern "C" fn queue_rq(
        arg1: *mut bindings::blk_mq_hw_ctx,
        bd: *const bindings::blk_mq_queue_data,
    ) -> bindings::blk_status_t {
        T::queue_rq(arg1, bd)
    }

    unsafe extern "C" fn complete(req: *mut bindings::request) {
        T::complete(req)
    }

    const OPS: bindings::blk_mq_ops = bindings::blk_mq_ops {
        queue_rq: Some(Self::queue_rq),
        commit_rqs: None,
        queue_rqs: None,
        put_budget: None,
        get_budget: None,
        set_rq_budget_token: None,
        get_rq_budget_token: None,
        timeout: None,
        poll: None,
        complete: Some(Self::complete),
        init_hctx: Some(Self::init_hctx),
        exit_hctx: None,
        init_request: Some(Self::init_request),
        exit_request: None,
        cleanup_rq: None,
        busy: None,
        map_queues: None,
        show_rq: None,
    };
}

trait BlkRqOps {
    fn init_request(
        _set: *mut bindings::blk_mq_tag_set,
        rq: *mut bindings::request,
        _arg2: core::ffi::c_uint,
        _arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int;

    fn init_hctx(
        _arg1: *mut bindings::blk_mq_hw_ctx,
        _arg2: *mut core::ffi::c_void,
        _arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int;

    fn queue_rq(
        arg1: *mut bindings::blk_mq_hw_ctx,
        bd: *const bindings::blk_mq_queue_data,
    ) -> bindings::blk_status_t;

    fn complete(req: *mut bindings::request);
}

struct BlkAdminOps;

impl BlkRqOps for BlkAdminOps {
    fn init_request(
        _set: *mut bindings::blk_mq_tag_set,
        _rq: *mut bindings::request,
        _arg2: core::ffi::c_uint,
        _arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int {
        0
    }

    fn init_hctx(
        _arg1: *mut bindings::blk_mq_hw_ctx,
        _arg2: *mut core::ffi::c_void,
        _arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int {
        0
    }

    fn queue_rq(
        _arg1: *mut bindings::blk_mq_hw_ctx,
        bd: *const bindings::blk_mq_queue_data,
    ) -> bindings::blk_status_t {
        unsafe {
            let req = (*bd).rq;
            bindings::blk_mq_start_request(req);
            let pdu = rq_to_pdu(req);
            let mut a: Box<NvmeQueue> = Box::from_raw((*(*req).q).queuedata as _);

            let mut tail = a.sq_tail as usize;

            core::ptr::copy(
                &((*pdu).cmnd),
                a.sq_cmds.add(tail * NvmeDriver::SQ_STRUCT_SIZE) as *mut NvmeCommonCommand,
                NvmeDriver::SQ_STRUCT_SIZE,
            );

            tail += 1;
            if tail == a.depth {
                tail = 0;
            }
            bindings::writel(tail as u32, a.q_db);
            a.sq_tail = tail as u16;

            (*(*req).q).queuedata = Box::into_raw(a) as _;
        }

        bindings::BLK_STS_OK as u8
    }

    fn complete(req: *mut bindings::request) {
        // let pdu = rq_to_pdu(req);
        // pr_info!(
        //     "admin complete {:?}, status: {}, result: {}",
        //     req,
        //     (*pdu).status,
        //     (*pdu).result
        // );
        unsafe {
            bindings::blk_mq_end_request(req, 0);
        }
    }
}

const CTRL_PAGE_SIZE: i32 = 4096;

struct BlkIoOps;

impl BlkRqOps for BlkIoOps {
    fn init_request(
        _set: *mut bindings::blk_mq_tag_set,
        _rq: *mut bindings::request,
        _arg2: core::ffi::c_uint,
        _arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int {
        0
    }

    fn init_hctx(
        _arg1: *mut bindings::blk_mq_hw_ctx,
        _arg2: *mut core::ffi::c_void,
        _arg3: core::ffi::c_uint,
    ) -> core::ffi::c_int {
        0
    }

    fn queue_rq(
        _arg1: *mut bindings::blk_mq_hw_ctx,
        bd: *const bindings::blk_mq_queue_data,
    ) -> bindings::blk_status_t {
        unsafe {
            let mut req = (*bd).rq;
            let mut pdu = rq_to_pdu(req);
            (*pdu).nents = 0;
            let mut a: Box<NvmeQueue> = Box::from_raw((*(*req).q).queuedata as _);
            let mut tail = a.sq_tail as usize;
            let mut sg_idx = 0;

            pr_info!("queue io request tail {}, {:?}", tail, req);
            bindings::blk_mq_start_request(req);
            let n = bindings::blk_rq_nr_phys_segments(req);
            let cmnd = a.sq_cmds.add(tail * NvmeDriver::SQ_STRUCT_SIZE) as *mut NvmeRwCommand;
            (*cmnd).command_id = (*req).tag as u16;
            (*cmnd).flags = 0;
            (*cmnd).nsid = 1;
            (*cmnd).rsvd2 = 0;
            (*cmnd).metadata = 0;
            (*cmnd).slba = bindings::blk_rq_pos(req);
            (*cmnd).length = ((bindings::blk_rq_bytes(req) >> 9) - 1) as u16;
            (*cmnd).control = 0;
            (*cmnd).dsmgmt = 0;
            (*cmnd).reftag = 0;
            (*cmnd).apptag = 0;
            (*cmnd).appmask = 0;
            (*cmnd).prp1 = 0;
            (*cmnd).prp2 = 0;

            if n > 0 {
                bindings::sg_init_table(&mut (*pdu).sg[sg_idx], n as u32);
                let nents = bindings::blk_rq_map_sg((*req).q, req, &mut (*pdu).sg[sg_idx]);
                let dir = if ((bindings::REQ_OP_MASK & (*req).cmd_flags) & 1) == 1 {
                    bindings::dma_data_direction_DMA_TO_DEVICE
                } else {
                    bindings::dma_data_direction_DMA_FROM_DEVICE
                };

                if dir == bindings::dma_data_direction_DMA_TO_DEVICE {
                    (*cmnd).opcode = NvmeDriver::CMD_WRITE;
                } else {
                    (*cmnd).opcode = NvmeDriver::CMD_READ;
                }

                let mapped = bindings::dma_map_sg_attrs(
                    a.dev,
                    &mut (*pdu).sg[sg_idx],
                    nents,
                    dir,
                    bindings::DMA_ATTR_NO_WARN as u64,
                );

                let mut sg = (*pdu).sg[sg_idx];
                let mut dma_len: i32 = sg.dma_length as i32;
                let mut dma_addr = sg.dma_address;
                let offset = (dma_addr % CTRL_PAGE_SIZE as u64) as i32;
                (*cmnd).prp1 = dma_addr;

                let mut length: i32 = bindings::blk_rq_payload_bytes(req) as i32;
                // pr_info!("len {}, dma mapped: {}, offset: {}", length, mapped, offset);
                (*pdu).nents = mapped as i32;
                length -= CTRL_PAGE_SIZE - offset;
                if length > 0 {
                    dma_len -= CTRL_PAGE_SIZE - offset;
                    if dma_len > 0 {
                        dma_addr += (CTRL_PAGE_SIZE - offset) as u64;
                    } else {
                        sg_idx += 1;
                        sg = (*pdu).sg[sg_idx];
                        dma_len = sg.dma_length as i32;
                        dma_addr = sg.dma_address;
                    }

                    if length > CTRL_PAGE_SIZE {
                        let mut prp_dma: u64 = 0;
                        let l: *mut u64 = bindings::dma_alloc_attrs(
                            a.dev,
                            8 * MAX_SG,
                            &mut prp_dma,
                            bindings::___GFP_ATOMIC,
                            0,
                        ) as _;
                        // pr_info!("alloc coherent {:?}", l);
                        (*cmnd).prp2 = prp_dma;
                        for i in 0..MAX_SG {
                            *(l.add(i)) = dma_addr;
                            dma_len -= CTRL_PAGE_SIZE;
                            dma_addr += CTRL_PAGE_SIZE as u64;
                            length -= CTRL_PAGE_SIZE;
                            // pr_info!("alloc coherent {} {:?} {} {}", i, l.add(i), dma_len, length);

                            if length <= 0 {
                                break;
                            }
                            if dma_len > 0 {
                                continue;
                            }
                            sg_idx += 1;
                            sg = (*pdu).sg[sg_idx];
                            dma_len = sg.dma_length as i32;
                            dma_addr = sg.dma_address;
                        }
                    } else {
                        // pr_info!("no coherent {}", dma_addr);
                        (*cmnd).prp2 = dma_addr;
                    }
                }

                tail += 1;
                if tail == (*a).depth {
                    tail = 0;
                }

                bindings::writel(tail as u32, a.q_db);
                a.sq_tail = tail as u16;
            }
            (*(*req).q).queuedata = Box::into_raw(a) as _;
        }
        bindings::BLK_STS_OK as u8
    }

    fn complete(req: *mut bindings::request) {
        let pdu = rq_to_pdu(req);
        pr_info!(
            "io complete {:?}, status: {}, result: {}",
            req,
            (*pdu).status,
            (*pdu).result
        );
        unsafe {
            let a: Box<NvmeQueue> = Box::from_raw((*(*req).q).queuedata as _);
            if (*pdu).nents > 0 {
                bindings::dma_unmap_sg_attrs(a.dev, &mut (*pdu).sg[0], (*pdu).nents, 2, 0);
            }
            bindings::blk_mq_end_request(req, 0);

            (*(*req).q).queuedata = Box::into_raw(a) as _;
        }
    }
}

struct Tag {
    set: bindings::blk_mq_tag_set,
    _pin: PhantomPinned,
}

impl Tag {
    fn new<T: BlkRqOps>() -> Self {
        let mut set: bindings::blk_mq_tag_set = unsafe { zeroed() };
        set.ops = &Adapter::<T>::OPS;

        Tag {
            set,
            _pin: PhantomPinned,
        }
    }
}

struct RequestQueue {
    ptr: *mut bindings::request_queue,
}

impl RequestQueue {
    fn new(tag: &mut Tag) -> Self {
        unsafe {
            tag.set.nr_hw_queues = 1;
            tag.set.queue_depth = 30;
            tag.set.timeout = 30 * bindings::HZ;
            tag.set.numa_node = -1;
            tag.set.cmd_size = core::mem::size_of::<Pdu>() as u32;
            tag.set.flags = bindings::BLK_MQ_F_NO_SCHED;
            let _r = bindings::blk_mq_alloc_tag_set(&mut tag.set);
        }

        let ptr = unsafe { bindings::blk_mq_init_queue(&mut tag.set) };
        if unsafe { bindings::IS_ERR(ptr as *const c_void) } {
            pr_info!("init queue error");
        }
        RequestQueue { ptr }
    }
}

#[repr(C)]
struct Pdu {
    cmnd: NvmeCommonCommand,
    status: u16,
    result: u32,
    // should be allocated dynamically
    nents: i32,
    sg: [bindings::scatterlist; MAX_SG],
}

const MAX_SG: usize = 16;

fn rq_to_pdu(rq: *mut bindings::request) -> *mut Pdu {
    let ptr = unsafe { bindings::blk_mq_rq_to_pdu(rq) };
    ptr as *mut Pdu
}

#[repr(C)]
struct NvmeCompletion {
    result: u32,
    _rsvd: u32,
    sq_head: u16,
    sq_id: u16,
    command_id: u16,
    status: u16,
}

#[repr(C)]
struct NvmeCommonCommand {
    opcode: u8,
    flags: u8,
    command_id: u16,
    nsid: u32,
    cdw2: [u32; 2],
    metadata: u64,
    prp1: u64,
    prp2: u64,
    cdw10: [u32; 6],
}

#[repr(C)]
struct NvmeIdentify {
    opcode: u8,
    flags: u8,
    command_id: u16,
    nsid: u32,
    rsvd2: [u64; 2],
    prp1: u64,
    prp2: u64,
    cns: u32,
    rsvd11: [u32; 5],
}

#[repr(C)]
struct NvmeFeatures {
    opcode: u8,
    flags: u8,
    command_id: u16,
    nsid: u32,
    rsvd2: [u64; 2],
    prp1: u64,
    prp2: u64,
    fib: u32,
    dword11: u32,
    rsvd11: [u32; 4],
}

#[repr(C)]
struct NvmeCreateCq {
    opcode: u8,
    flags: u8,
    command_id: u16,
    rsvd1: [u32; 5],
    prp1: u64,
    rsvd8: u64,
    cqid: u16,
    qsize: u16,
    cq_flags: u16,
    irq_vector: u16,
    rsvd12: [u32; 4],
}

#[repr(C)]
struct NvmeCreateSq {
    opcode: u8,
    flags: u8,
    command_id: u16,
    rsvd1: [u32; 5],
    prp1: u64,
    rsvd8: u64,
    sqid: u16,
    qsize: u16,
    sq_flags: u16,
    cqid: u16,
    rsvd12: [u32; 4],
}

#[repr(C)]
struct NvmeIdPowerState {
    max_power: u16,
    rsvd2: u16,
    entry_lat: u32,
    exit_lat: u32,
    read_tput: u8,
    read_lat: u8,
    write_tput: u8,
    write_lat: u8,
    rsvd16: [u8; 16],
}

#[repr(C)]
struct NvmeIdCtrl {
    vid: u16,
    ssvid: u16,
    sn: [u8; 20],
    mn: [u8; 40],
    fr: [u8; 8],
    rab: u8,
    ieee: [u8; 3],
    mic: u8,
    mdts: u8,
    rsvd78: [u8; 178],
    oacs: u16,
    acl: u8,
    aerl: u8,
    frmw: u8,
    lpa: u8,
    elpe: u8,
    npss: u8,
    rsvd264: [u8; 248],
    sqes: u8,
    cqes: u8,
    rsvd514: [u8; 2],
    nn: u32,
    oncs: u16,
    fuses: u16,
    fna: u8,
    vwc: u8,
    awun: u16,
    awupf: u16,
    rsvd530: [u8; 1518],
    psd: [NvmeIdPowerState; 32],
    vs: [u8; 1024],
}

#[repr(C)]
struct NvmeRwCommand {
    opcode: u8,
    flags: u8,
    command_id: u16,
    nsid: u32,
    rsvd2: u64,
    metadata: u64,
    prp1: u64,
    prp2: u64,
    slba: u64,
    length: u16,
    control: u16,
    dsmgmt: u32,
    reftag: u32,
    apptag: u16,
    appmask: u16,
}

#[repr(C)]
struct NvmeLbaf {
    ms: u16,
    ds: u8,
    rp: u8,
}

#[repr(C)]
struct NvmeIdNs {
    nsze: u64,
    ncap: u64,
    nuse: u64,
    nsfeat: u8,
    nlbaf: u8,
    flbas: u8,
    mc: u8,
    dpc: u8,
    dps: u8,
    rsvd30: [u8; 98],
    lbaf: [NvmeLbaf; 16],
    rsvd192: [u8; 192],
    vs: [u8; 3712],
}

#[repr(C)]
struct NvmeLbaRangeType {
    rt_type: u8,
    attributes: u8,
    rsvd2: [u8; 14],
    slba: u64,
    nlb: u64,
    guid: [u8; 16],
    rsvd48: [u8; 16],
}

struct DevInfo {
    bar: *mut core::ffi::c_void,

    admin_rq: RequestQueue,
    admin_irq: irq::Registration<NvmeDriver>,

    io_irq: irq::Registration<NvmeDriver>,
}

unsafe impl Send for DevInfo {}

unsafe impl Sync for DevInfo {}

impl driver::DeviceRemoval for DevInfo {
    fn device_remove(&self) {}
}

struct NvmeQueue {
    cq_dma_addr: u64,
    cqes: *mut c_void,

    sq_dma_addr: u64,
    sq_cmds: *mut c_void,
    sq_tail: u16,
    depth: usize,
    cq_head: u32,
    cq_phase: u16,

    q_db: *mut c_void,

    dev: *mut bindings::device,
}

impl NvmeQueue {
    fn new(pdev: &mut pcidev::Device, depth: usize, q_db: *mut c_void) -> Self {
        let mut cq_dma_addr: u64 = 0;
        let cqes = pdev
            .dma_alloc_coherent(
                depth * NvmeDriver::CQ_STRUCT_SIZE,
                &mut cq_dma_addr,
                bindings::GFP_KERNEL,
            )
            .unwrap();
        let mut sq_dma_addr: u64 = 0;
        let sq_cmds = pdev
            .dma_alloc_coherent(
                depth * NvmeDriver::SQ_STRUCT_SIZE,
                &mut sq_dma_addr,
                bindings::GFP_KERNEL,
            )
            .unwrap();

        NvmeQueue {
            dev: unsafe { &mut (*pdev.ptr).dev },
            cq_dma_addr,
            cqes,
            sq_dma_addr,
            sq_cmds,
            sq_tail: 0,
            depth,
            q_db,
            cq_phase: 1,
            cq_head: 0,
        }
    }
}

struct NvmeIrq {
    tag: Tag,
    cq_head: u32,
    cq_phase: u16,
    depth: usize,

    cqes: *mut c_void,
    q_db: *mut c_void,
}

impl irq::Handler for NvmeDriver {
    type Data = Ref<SpinLock<NvmeIrq>>;

    fn handle_irq(data: RefBorrow<'_, SpinLock<NvmeIrq>>) -> irq::Return {
        unsafe {
            let mut q = data.lock();
            let mut head = q.cq_head;
            let mut phase = q.cq_phase;
            let maps = core::slice::from_raw_parts_mut(q.tag.set.tags, 1);

            loop {
                let cqe =
                    q.cqes.add(head as usize * NvmeDriver::CQ_STRUCT_SIZE) as *mut NvmeCompletion;

                let status = (*cqe).status;
                pr_info!(
                    "irq: head {}, phase {}, status {}, id {}",
                    head,
                    phase,
                    status,
                    (*cqe).command_id
                );
                if (status & 1) != phase {
                    break;
                }

                let rq = bindings::blk_mq_tag_to_rq(maps[0], (*cqe).command_id as u32);

                if !rq.is_null() {
                    let pdu = rq_to_pdu(rq);
                    (*pdu).result = (*cqe).result;
                    (*pdu).status = (*cqe).status >> 1;

                    bindings::blk_mq_complete_request(rq);
                }

                head += 1;
                if head == q.depth as u32 {
                    head = 0;
                    phase = !phase;
                }
            }

            if q.cq_head == head && phase == q.cq_phase {
                return irq::Return::None;
            }

            bindings::writel(head, q.q_db);
            q.cq_head = head;
            q.cq_phase = phase;
        }
        irq::Return::Handled
    }
}

struct NvmeDriver;

impl NvmeDriver {
    const NVME_CC_ENABLE: u32 = 1 << 0;
    const NVME_CC_CSS_NVM: u32 = 0 << 4;
    const NVME_CC_MPS_SHIFT: u32 = 7;
    const NVME_CC_ARB_RR: u32 = 0 << 11;
    // const NVME_CC_ARB_WRRU: u32 = 1 << 11;
    // const NVME_CC_ARB_VS: u32 = 7 << 11;
    const NVME_CC_SHN_NONE: u32 = 0 << 14;
    // const NVME_CC_SHN_NORMAL: u32 = 1 << 14;
    // const NVME_CC_SHN_ABRUPT: u32 = 2 << 14;
    const NVME_CC_IOSQES: u32 = 6 << 16;
    const NVME_CC_IOCQES: u32 = 4 << 20;
    // const NVME_CSTS_RDY: u32 = 1 << 0;
    // const NVME_CSTS_CFS: u32 = 1 << 1;
    // const NVME_CSTS_SHST_NORMAL: u32 = 0 << 2;
    // const NVME_CSTS_SHST_OCCUR: u32 = 1 << 2;
    // const NVME_CSTS_SHST_CMPLT: u32 = 2 << 2;

    const IOMEM_SIZE: u64 = 8192;
    const QUEUE_DEPTH: usize = 64;
    const SQ_STRUCT_SIZE: usize = 64;
    const CQ_STRUCT_SIZE: usize = 16;

    const OFFSET_CAP: usize = 0;
    // const OFFSET_VS: usize = 8;
    // const OFFSET_INTMS: usize = 12;
    // const OFFSET_INTMC: usize = 16;
    const OFFSET_CC: usize = 20;
    const OFFSET_CSTS: usize = 28;
    const OFFSET_AQA: usize = 36;
    const OFFSET_ASQ: usize = 40;
    const OFFSET_ACQ: usize = 48;

    const CMD_WRITE: u8 = 0x01;
    const CMD_READ: u8 = 0x02;

    // admin opcode
    const ADMIN_CREATE_SQ: u8 = 0x01;
    const ADMIN_CREATE_CQ: u8 = 0x05;
    const ADMIN_IDENTIFY: u8 = 0x06;
    const ADMIN_SET_FEATURES: u8 = 0x09;
    const ADMIN_GET_FEATURES: u8 = 0x0a;

    // misc
    const QUEUE_PHYS_CONTIG: u16 = (1 << 0);
    const CQ_IRQ_ENABLED: u16 = (1 << 1);
    const SQ_PRIO_MEDIUM: u16 = (2 << 1);

    const FEAT_LBA_RANGE: u32 = 0x03;
    const FEAT_NUM_QUEUES: u32 = 0x07;

    fn execute_cmnd(rq: &mut RequestQueue, cmnd: &NvmeCommonCommand) -> u32 {
        unsafe {
            let mut rq = {
                let rw = bindings::req_opf_REQ_OP_DRV_IN;
                let rq = bindings::blk_mq_alloc_request(rq.ptr, rw, 0);
                let err = bindings::IS_ERR(rq as *const c_void);
                let mut pdu = rq_to_pdu(rq);
                if err {
                    pr_info!("alloc error {}", bindings::PTR_ERR(rq as *const c_void));
                }

                if pdu.is_null() {
                    pr_info!("pdu is null!");
                } else {
                    //core::ptr::copy(cmnd, &mut ((*pdu).cmnd), NvmeDriver::SQ_STRUCT_SIZE);
                    (*pdu).cmnd.opcode = cmnd.opcode;
                    (*pdu).cmnd.flags = cmnd.flags;
                    (*pdu).cmnd.command_id = (*rq).tag as u16;
                    (*pdu).cmnd.nsid = cmnd.nsid;
                    (*pdu).cmnd.cdw2 = cmnd.cdw2;
                    (*pdu).cmnd.metadata = cmnd.metadata;
                    (*pdu).cmnd.prp1 = cmnd.prp1;
                    (*pdu).cmnd.prp2 = cmnd.prp2;
                    (*pdu).cmnd.cdw10 = cmnd.cdw10;
                }
                rq
            };

            (*rq).rq_flags |= bindings::req_flag_bits___REQ_FAILFAST_DRIVER;
            (*rq).rq_flags |= 1 << 7;

            let _status = bindings::blk_execute_rq(rq, false);
            let pdu = rq_to_pdu(rq);
            let result = (*pdu).result;
            bindings::blk_mq_free_request(rq);
            result
        }
    }
}

impl pcidev::Driver for NvmeDriver {
    type Data = Box<DevInfo>;

    // for simplicity, probe() does everything synchronously
    fn probe(pdev: &mut pcidev::Device) -> Result<Self::Data> {
        pr_info!("rustnvme probe");

        const NR_IRQS: u32 = 2;

        let bars = pdev.select_bars(bindings::IORESOURCE_MEM.into());
        pdev.request_selected_regions(bars, c_str!("nvme"))?;

        let bar = unsafe { bindings::ioremap(pdev.resource_start(0), NvmeDriver::IOMEM_SIZE) };
        let dbs = unsafe { bar.add(4096) };

        pdev.enable_device_mem()?;
        pdev.set_master();

        unsafe {
            let n = bindings::pci_alloc_irq_vectors_affinity(
                pdev.ptr,
                NR_IRQS,
                NR_IRQS,
                bindings::PCI_IRQ_ALL_TYPES,
                ptr::null_mut(),
            );
            assert_eq!(n, NR_IRQS as i32);
        }

        pdev.dma_set_mask(!0)?;
        pdev.dma_set_coherent_mask(!0)?;

        let admin_queue = Box::try_new(NvmeQueue::new(pdev, NvmeDriver::QUEUE_DEPTH, unsafe {
            bar.add(4096)
        }))
        .unwrap();

        let mut admin_tag = Tag::new::<BlkAdminOps>();
        let mut admin_rq = RequestQueue::new(&mut admin_tag);

        let mut aqa: u32 = (NvmeDriver::QUEUE_DEPTH - 1).try_into().unwrap();
        aqa |= aqa << 16;

        let mut ctrl_config: u32 = NvmeDriver::NVME_CC_ENABLE | NvmeDriver::NVME_CC_CSS_NVM;
        ctrl_config |= (bindings::PAGE_SHIFT - 12) << NvmeDriver::NVME_CC_MPS_SHIFT;
        ctrl_config |= NvmeDriver::NVME_CC_ARB_RR | NvmeDriver::NVME_CC_SHN_NONE;
        ctrl_config |= NvmeDriver::NVME_CC_IOSQES | NvmeDriver::NVME_CC_IOCQES;

        unsafe {
            bindings::writel(0, bar.add(NvmeDriver::OFFSET_CC));
            bindings::writel(aqa, bar.add(NvmeDriver::OFFSET_AQA));
            bindings::writeq(admin_queue.sq_dma_addr, bar.add(NvmeDriver::OFFSET_ASQ));
            bindings::writeq(admin_queue.cq_dma_addr, bar.add(NvmeDriver::OFFSET_ACQ));
            bindings::writel(ctrl_config, bar.add(NvmeDriver::OFFSET_CC));
        }
        let cap = unsafe { bindings::readq(bar.add(NvmeDriver::OFFSET_CAP)) };
        let _db_stride = (cap >> 32) & 0xf;

        let mut ready = 0;
        for _ in 0..10 {
            ready = unsafe { bindings::readl(bar.add(NvmeDriver::OFFSET_CSTS)) };
            if ready > 0 {
                break;
            }
            unsafe {
                bindings::msleep(100);
            }
        }
        assert!(ready > 0);

        let mut admin_irq_data = unsafe {
            SpinLock::new(NvmeIrq {
                tag: admin_tag,
                cq_head: 0,
                cq_phase: 1,
                cqes: admin_queue.cqes,
                depth: admin_queue.depth,
                q_db: admin_queue.q_db.add(4),
            })
        };
        kernel::spinlock_init!(
            unsafe { Pin::new_unchecked(&mut admin_irq_data) },
            "nvme_admin_irq"
        );
        unsafe {
            (*admin_rq.ptr).queuedata = Box::into_raw(admin_queue) as _;
        }

        let admin_irq = irq::Registration::<NvmeDriver>::try_new(
            unsafe { bindings::pci_irq_vector(pdev.ptr, 0) } as u32,
            Ref::try_new(admin_irq_data).unwrap(),
            irq::flags::SHARED,
            fmt!("nvme admin irq"),
        )
        .unwrap();

        {
            let mut cmnd: NvmeCommonCommand = unsafe { zeroed() };
            let mut c: NvmeFeatures = unsafe { zeroed() };
            c.opcode = NvmeDriver::ADMIN_SET_FEATURES;
            c.fib = NvmeDriver::FEAT_NUM_QUEUES;
            c.dword11 = 0;

            unsafe {
                core::ptr::copy(
                    &c,
                    &mut cmnd as *mut _ as *mut NvmeFeatures,
                    NvmeDriver::SQ_STRUCT_SIZE,
                );
            }

            let r = NvmeDriver::execute_cmnd(&mut admin_rq, &cmnd);
            pr_info!("result {}", r);
        }
        let io_queue_depth = unsafe {
            let a = (bindings::readq(bar.add(NvmeDriver::OFFSET_CAP)) & 0xffff) + 1;
            pr_info!("queue depth {}", core::cmp::min(a, 1024));
            core::cmp::min(a, 1024)
        };

        let io_queue = Box::try_new(NvmeQueue::new(
            pdev,
            io_queue_depth.try_into().unwrap(),
            unsafe { dbs.add(8) },
        ))
        .unwrap();

        let mut io_tag = Tag::new::<BlkIoOps>();
        unsafe {
            io_tag.set.nr_hw_queues = 1;

            io_tag.set.queue_depth = io_queue_depth as u32 - 1;
            io_tag.set.timeout = 30 * bindings::HZ;
            io_tag.set.numa_node = -1;
            io_tag.set.cmd_size = core::mem::size_of::<Pdu>() as u32;
            let _r = bindings::blk_mq_alloc_tag_set(&mut io_tag.set);
        }

        {
            let mut cmnd: NvmeCommonCommand = unsafe { zeroed() };
            let mut c: NvmeCreateCq = unsafe { zeroed() };
            c.opcode = NvmeDriver::ADMIN_CREATE_CQ;
            c.prp1 = io_queue.cq_dma_addr;
            c.cqid = 1;
            c.qsize = io_queue.depth as u16 - 1;
            c.cq_flags = NvmeDriver::QUEUE_PHYS_CONTIG | NvmeDriver::CQ_IRQ_ENABLED;
            c.irq_vector = 1;

            unsafe {
                core::ptr::copy(
                    &c,
                    &mut cmnd as *mut _ as *mut NvmeCreateCq,
                    NvmeDriver::SQ_STRUCT_SIZE,
                );
            }

            let r = NvmeDriver::execute_cmnd(&mut admin_rq, &cmnd);
            pr_info!("ioirq result2 {}", r);
        }
        {
            let mut cmnd: NvmeCommonCommand = unsafe { zeroed() };
            let mut c: NvmeCreateSq = unsafe { zeroed() };
            c.opcode = NvmeDriver::ADMIN_CREATE_SQ;
            c.prp1 = io_queue.sq_dma_addr;
            c.sqid = 1;
            c.qsize = io_queue.depth as u16 - 1;
            c.sq_flags = NvmeDriver::QUEUE_PHYS_CONTIG | NvmeDriver::SQ_PRIO_MEDIUM;
            c.cqid = 1;

            unsafe {
                core::ptr::copy(
                    &c,
                    &mut cmnd as *mut _ as *mut NvmeCreateSq,
                    NvmeDriver::SQ_STRUCT_SIZE,
                );
            }

            let r = NvmeDriver::execute_cmnd(&mut admin_rq, &cmnd);
            pr_info!("ioirq result3 {}", r);
        }

        let mut disks = Vec::try_with_capacity(1).unwrap();
        let io_queue_db = unsafe { io_queue.q_db.add(4) };
        let io_queue_depth = io_queue.depth;
        let io_queue_cqes = io_queue.cqes;
        {
            let mut dma_addr: u64 = 0;
            let mem = pdev
                .dma_alloc_coherent(8192, &mut dma_addr, bindings::GFP_KERNEL)
                .unwrap();

            let mut cmnd: NvmeCommonCommand = unsafe { zeroed() };
            let mut c: NvmeIdentify = unsafe { zeroed() };
            c.opcode = NvmeDriver::ADMIN_IDENTIFY;
            c.nsid = 0;
            c.prp1 = dma_addr;
            c.cns = 1;
            unsafe {
                core::ptr::copy(
                    &c,
                    &mut cmnd as *mut _ as *mut NvmeIdentify,
                    NvmeDriver::SQ_STRUCT_SIZE,
                );
            }

            let r = NvmeDriver::execute_cmnd(&mut admin_rq, &cmnd);
            pr_info!("result3 {}", r);

            let ctrl: *mut NvmeIdCtrl = mem as *mut NvmeIdCtrl;
            let mut model: [u8; 41] = [0; 41];
            for i in 0..40 {
                model[i] = unsafe { (*ctrl).mn[i] };
            }

            pr_info!("model: {}", CStr::from_bytes_with_nul(&model).unwrap());
            pr_info!("nn {}, mdts {}", (*ctrl).nn, (*ctrl).mdts);

            // handle only one ns

            let io_queue_ptr = Box::into_raw(io_queue) as _;
            let nn = 1;
            for i in 1..=nn {
                let mut cmnd: NvmeCommonCommand = unsafe { zeroed() };
                let mut c: NvmeIdentify = unsafe { zeroed() };
                c.opcode = NvmeDriver::ADMIN_IDENTIFY;
                c.nsid = i;
                c.prp1 = dma_addr;
                c.cns = 0;
                unsafe {
                    core::ptr::copy(
                        &c,
                        &mut cmnd as *mut _ as *mut NvmeIdentify,
                        NvmeDriver::SQ_STRUCT_SIZE,
                    );
                }

                let r = NvmeDriver::execute_cmnd(&mut admin_rq, &cmnd);
                pr_info!("nn result {} {}", i, r);

                let id_ns: *mut NvmeIdNs = mem as *mut NvmeIdNs;
                pr_info!("ns {} {}", i, (*id_ns).ncap);

                {
                    let mut cmnd: NvmeCommonCommand = unsafe { zeroed() };
                    let mut c: NvmeFeatures = unsafe { zeroed() };
                    c.opcode = NvmeDriver::ADMIN_GET_FEATURES;
                    c.nsid = i;
                    c.prp1 = dma_addr + 4096;
                    c.fib = NvmeDriver::FEAT_LBA_RANGE;
                    unsafe {
                        core::ptr::copy(
                            &c,
                            &mut cmnd as *mut _ as *mut NvmeFeatures,
                            NvmeDriver::SQ_STRUCT_SIZE,
                        );
                    }

                    let r = NvmeDriver::execute_cmnd(&mut admin_rq, &cmnd);
                    pr_info!("nn result {} {}", i, r);

                    let _rt: *mut NvmeLbaRangeType =
                        unsafe { mem.add(4096) as *mut NvmeLbaRangeType };

                    unsafe {
                        let lbaf = (*id_ns).flbas & 0xf;
                        let lba_shift = (*id_ns).lbaf[lbaf as usize].ds;
                        let capacity = (*id_ns).nsze << (lba_shift - 9);
                        let mut disk = bindings::blk_mq_alloc_disk(&mut io_tag.set, io_queue_ptr);
                        pr_info!(
                            "queue: {:?}, depth: {}",
                            (*disk).queue,
                            (*(*disk).queue).queue_depth
                        );
                        bindings::blk_queue_max_segments((*disk).queue, MAX_SG as u16);
                        (*disk).major = MAJOR;
                        (*disk).minors = 64;
                        let name = c_str!("nvme0n1");
                        for i in 0..name.len() {
                            (*disk).disk_name[i] = name[i] as i8;
                        }
                        (*disk).fops = &BlkDevOps::OPS;
                        bindings::set_capacity(disk, capacity);
                        disks.try_push(disk).unwrap();
                    }
                }
            }
        }

        let mut io_irq_data = unsafe {
            SpinLock::new(NvmeIrq {
                tag: io_tag,
                cq_head: 0,
                cq_phase: 1,
                cqes: io_queue_cqes,
                depth: io_queue_depth,
                q_db: io_queue_db,
            })
        };
        kernel::spinlock_init!(
            unsafe { Pin::new_unchecked(&mut io_irq_data) },
            "nvme_io_irq"
        );

        let io_irq = irq::Registration::<NvmeDriver>::try_new(
            unsafe { bindings::pci_irq_vector(pdev.ptr, 1) } as u32,
            Ref::try_new(io_irq_data).unwrap(),
            irq::flags::SHARED,
            fmt!("nvme io irq"),
        )
        .unwrap();
        for d in disks {
            unsafe { bindings::device_add_disk(&mut (*pdev.ptr).dev, d, ptr::null_mut()) };
        }

        pr_info!("end of probe");
        let info = Box::try_new(DevInfo {
            bar,
            admin_rq,
            admin_irq,
            io_irq,
        })?;
        Ok(info)
    }

    const PCI_ID_TABLE: &'static [bindings::pci_device_id] = &[
        bindings::pci_device_id {
            class: 0x010802,
            class_mask: 0xffffff,
            vendor: !0,
            device: !0,
            subvendor: !0,
            subdevice: !0,
            driver_data: 0,
            override_only: 0,
        },
        bindings::pci_device_id {
            class: 0,
            class_mask: 0,
            vendor: 0,
            device: 0,
            subvendor: 0,
            subdevice: 0,
            driver_data: 0,
            override_only: 0,
        },
    ];
}

static mut MAJOR: i32 = 0;

struct RustNvme {
    _driver: Pin<Box<driver::Registration<pcidev::Adapter<NvmeDriver>>>>,
}

impl kernel::Module for RustNvme {
    fn init(name: &'static CStr, module: &'static ThisModule) -> Result<Self> {
        unsafe {
            MAJOR = bindings::__register_blkdev(0, c_str!("nvme").as_char_ptr(), None);
        }
        let _driver =
            driver::Registration::<pcidev::Adapter<NvmeDriver>>::new_pinned(name, module)?;
        Ok(RustNvme { _driver })
    }
}

module! {
    type: RustNvme,
    name: b"rust_nvme",
    author: b"FUJITA Tomonori <fujita.tomonori@gmail.com>",
    description: b"Rust simple NVMe driver",
    license: b"GPL v2",
}

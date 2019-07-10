mod error;
mod plot;
mod stats;

use std::{
    convert::TryInto,
    fs,
    io::{self, Write},
    iter, path,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    thread, time,
};

use regex::Regex;
use structopt::StructOpt;
#[macro_use]
extern crate lazy_static;

use crate::error::DiskioError;
use crate::stats::Stats;

#[derive(Debug, StructOpt, Clone)]
struct Opt {
    path: String,

    #[structopt(long = "block-size", default_value = "1024")]
    block_size: SizeArg,

    #[structopt(long = "data-size", default_value = "1073741824")]
    data_size: SizeArg,

    #[structopt(long = "threads", default_value = "1")]
    nthreads: isize,
}

struct Context {
    fd: fs::File,
    block: Vec<u8>,
    data_size: isize,
}

impl Context {
    fn new(block_size: isize, data_size: isize, fd: fs::File) -> Context {
        Context {
            fd,
            block: {
                let mut block = Vec::with_capacity(block_size as usize);
                block.resize(block.capacity(), 0xAB);
                block
            },
            data_size,
        }
    }
}

impl Context {
    fn make_data_file(id: isize, opt: &Opt) -> io::Result<fs::File> {
        let filepath = {
            // create dir
            let mut p = path::PathBuf::new();
            p.push(&opt.path);
            fs::create_dir_all(p.as_path())?;
            // remove file
            p.push(format!("diskio-{}.data", id));
            fs::remove_file(p.as_path()).ok();
            p
        };

        println!("creating file `{}` ..", filepath.to_str().unwrap());
        Ok(fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(filepath.as_path())?)
    }

    fn path_latency_plot(opt: &Opt, bsize: isize, dsize: isize) -> path::PathBuf {
        let mut p = path::PathBuf::new();
        p.push(&opt.path);
        p.push(format!(
            "diskio-plot-latency-{}x{}x{}.png",
            opt.nthreads,
            humanize(bsize.try_into().unwrap()),
            humanize(dsize.try_into().unwrap())
        ));
        p
    }

    fn path_throughput_plot(opt: &Opt, bsize: isize, dsize: isize) -> path::PathBuf {
        let mut p = path::PathBuf::new();
        p.push(&opt.path);
        p.push(format!(
            "diskio-plot-throughput-{}x{}x{}.png",
            opt.nthreads,
            humanize(bsize.try_into().unwrap()),
            humanize(dsize.try_into().unwrap())
        ));
        p
    }
}

static TOTAL: AtomicU64 = AtomicU64::new(0);

fn main() {
    let opt = Opt::from_args();
    let xs = opt
        .clone()
        .data_size
        .get_datas()
        .iter()
        .map(|d| {
            iter::repeat(*d)
                .zip(opt.clone().block_size.get_blocks())
                .collect::<Vec<(isize, isize)>>()
        })
        .flatten()
        .collect::<Vec<(isize, isize)>>();

    for (dsize, bsize) in xs {
        let mut writers = vec![];
        let start_time = time::SystemTime::now();
        for i in 0..opt.nthreads {
            let fd = Context::make_data_file(i, &opt).unwrap();
            let ctxt = Context::new(bsize, dsize / opt.nthreads, fd);
            writers.push(thread::spawn(move || writer_thread(ctxt)));
        }

        let mut ss = Stats::new();
        for (i, w) in writers.into_iter().enumerate() {
            match w.join() {
                Ok(res) => match res {
                    Ok(stat) => ss.join(stat),
                    Err(err) => println!("thread {} errored: {}", i, err),
                },
                Err(_) => println!("thread {} paniced", i),
            }
        }
        plot::latency(
            Context::path_latency_plot(&opt, bsize, dsize),
            format!(
                "fd.sync_all() latency, block-size:{}, threads:{}",
                humanize(bsize.try_into().unwrap()),
                opt.nthreads,
            ),
            ss.sync_latencies,
        )
        .expect("unable to plot latency");

        plot::throughput(
            Context::path_throughput_plot(&opt, bsize, dsize),
            format!(
                "throughput for block-size:{}, threads:{}",
                humanize(bsize.try_into().unwrap()),
                opt.nthreads,
            ),
            ss.throughputs,
        )
        .expect("unable to plot latency");

        let elapsed = start_time.elapsed().expect("failed to compute elapsed");
        let total: usize = TOTAL.load(Ordering::Relaxed).try_into().unwrap();
        println!(
            "wrote {} using {} threads with {} block-size in {:?}\n",
            humanize(total),
            opt.nthreads,
            humanize(bsize.try_into().unwrap()),
            elapsed
        );
        TOTAL.store(0, Ordering::Relaxed);
    }
}

fn writer_thread(mut ctxt: Context) -> Result<Stats, DiskioError> {
    let mut stats = Stats::new();
    while ctxt.data_size > 0 {
        let start_time = time::SystemTime::now();
        match ctxt.fd.write(ctxt.block.as_slice()) {
            Ok(n) if n != ctxt.block.len() => {
                let msg = format!("partial write {}", n);
                Err(DiskioError(msg))
            }
            Err(err) => {
                let msg = format!("invalid write `{:?}`", err);
                Err(DiskioError(msg))
            }
            _ => Ok(()),
        }?;
        ctxt.fd.sync_all()?;
        ctxt.data_size -= {
            let n: isize = ctxt.block.len().try_into().unwrap();
            n
        };
        stats.click(start_time, ctxt.block.len().try_into().unwrap())?;
    }

    let n: u64 = ctxt.fd.metadata().unwrap().len().try_into().unwrap();
    TOTAL.fetch_add(n, Ordering::Relaxed);
    Ok(stats)
}

fn humanize(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < (1024 * 1024) {
        format!("{}KB", bytes / 1024)
    } else if bytes < (1024 * 1024 * 1024) {
        format!("{}MB", bytes / (1024 * 1024))
    } else if bytes < (1024 * 1024 * 1024 * 1024) {
        format!("{}GB", bytes / (1024 * 1024 * 1024))
    } else {
        format!("{}TB", bytes / (1024 * 1024 * 1024 * 1024))
    }
}

#[derive(Debug, Clone)]
enum SizeArg {
    None,
    Range(Option<isize>, Option<isize>),
    List(Vec<isize>),
}

lazy_static! {
    static ref ARG_RE1: Regex = Regex::new(r"^([0-9]+[kKmMgG]?)(\.\.[0-9]+[kKmMgG]?)?$").unwrap();
    static ref ARG_RE2: Regex = Regex::new(r"^([0-9]+[kKmMgG]?)(,[0-9]+[kKmMgG]?)*$").unwrap();
    static ref BLOCK_SIZES: [isize; 9] = [
        128,
        256,
        512,
        1024,
        10 * 1024,
        100 * 1024,
        1024 * 1024,
        10 * 1024 * 1024,
        100 * 1024 * 1024,
    ];
    static ref DATA_SIZES: [isize; 6] = [
        1 * 1024 * 1024,
        10 * 1024 * 1024,
        100 * 1024 * 1024,
        1024 * 1024 * 1024,
        10 * 1024 * 1024 * 1024,
        100 * 1024 * 1024 * 1024,
    ];
}

impl FromStr for SizeArg {
    type Err = String;

    fn from_str(s: &str) -> Result<SizeArg, Self::Err> {
        //println!("re1 {}", s);
        match ARG_RE1.captures(s) {
            None => (),
            Some(captrs) => {
                let x = captrs.get(1).map(|m| SizeArg::to_isize(m.as_str()));
                let y = captrs.get(2).map(|m| {
                    SizeArg::to_isize(m.as_str().chars().skip(2).collect::<String>().as_str())
                });
                // println!("re1 {}, {:?} {:?}", s, x, y);
                return Ok(SizeArg::Range(x.transpose()?, y.transpose()?));
            }
        };
        //println!("re2 {}", s);
        match ARG_RE2.captures(s) {
            None => Ok(SizeArg::None),
            Some(captrs) => {
                let sizes = captrs
                    .get(0)
                    .unwrap()
                    .as_str()
                    .split(',')
                    .map(|s| SizeArg::to_isize(s).unwrap())
                    .collect::<Vec<isize>>();
                return Ok(SizeArg::List(sizes));
            }
        }
    }
}

impl SizeArg {
    fn to_isize(s: &str) -> Result<isize, String> {
        let chs: Vec<char> = s.chars().collect();
        let (s, amp) = match chs[chs.len() - 1] {
            'k' | 'K' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024)
            }
            'm' | 'M' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024 * 1024)
            }
            'g' | 'G' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024 * 1024 * 1024)
            }
            't' | 'T' => {
                let s: String = chs[..(chs.len() - 1)].iter().collect();
                (s, 1024 * 1024 * 1024 * 1024)
            }
            _ => {
                let s: String = chs[..chs.len()].iter().collect();
                (s, 1)
            }
        };
        // println!("{}", s);
        match s.parse::<isize>() {
            Err(err) => Err(format!("parse: {:?}", err)),
            Ok(n) => Ok(n * amp),
        }
    }

    fn get_blocks(self) -> Vec<isize> {
        let (from, till) = match self {
            SizeArg::None => return vec![],
            SizeArg::List(sizes) => return sizes,
            SizeArg::Range(None, None) => return vec![],
            SizeArg::Range(Some(x), None) => return vec![x],
            SizeArg::Range(None, Some(_)) => unreachable!(),
            SizeArg::Range(Some(x), Some(y)) => (x, y),
        };
        BLOCK_SIZES
            .clone()
            .iter()
            .skip_while(|x| **x < from)
            .take_while(|x| **x <= till)
            .map(|x| *x)
            .collect()
    }

    fn get_datas(self) -> Vec<isize> {
        let (from, till) = match self {
            SizeArg::None => return vec![],
            SizeArg::List(sizes) => return sizes,
            SizeArg::Range(None, None) => return vec![],
            SizeArg::Range(Some(x), None) => return vec![x],
            SizeArg::Range(None, Some(_)) => unreachable!(),
            SizeArg::Range(Some(x), Some(y)) => (x, y),
        };
        DATA_SIZES
            .clone()
            .iter()
            .skip_while(|x| **x < from)
            .take_while(|x| **x <= till)
            .map(|x| *x)
            .collect()
    }
}

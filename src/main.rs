// TODO: plots
// a. latency histogram plot.
// b. throughput moving average plot.
// c. repeat (a) and (b) for different blocksizes and datasizes
// d. repeat (a), (b) and (c) with different 1,2,4,8 writers.

use std::{
    convert::TryInto,
    error::Error,
    fmt, fs,
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

mod plot;

#[derive(Debug, StructOpt, Clone)]
struct Opt {
    path: String,

    #[structopt(long = "block-size", default_value = "1024")]
    block_size: SizeRange,

    #[structopt(long = "data-size", default_value = "1073741824")]
    data_size: SizeRange,

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
            opt.nthreads, bsize, dsize
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

        let mut stats = Stats::new();
        for (i, w) in writers.into_iter().enumerate() {
            match w.join() {
                Ok(res) => match res {
                    Ok(stat) => stats.join(stat),
                    Err(err) => println!("thread {} errored: {}", i, err),
                },
                Err(_) => println!("thread {} paniced", i),
            }
        }

        plot::latency(
            Context::path_latency_plot(&opt, bsize, dsize),
            stats.latencies,
        )
        .expect("unable to plot latency");

        let elapsed = start_time.elapsed().expect("failed to compute elapsed");
        let total: usize = TOTAL.load(Ordering::Relaxed).try_into().unwrap();
        println!(
            "wrote {} across {} threads with {} block-size in {:?}\n",
            humanize(total),
            opt.nthreads,
            bsize,
            elapsed
        );
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
        stats
            .latencies
            .push(start_time.elapsed()?.as_micros().try_into().unwrap());
    }
    let n: u64 = ctxt.fd.metadata().unwrap().len().try_into().unwrap();
    TOTAL.fetch_add(n, Ordering::Relaxed);
    Ok(stats)
}

fn humanize(bytes: usize) -> String {
    if bytes < (1024 * 1024) {
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
struct SizeRange(Option<isize>, Option<isize>);

lazy_static! {
    static ref ARG_RE: Regex = Regex::new("([0-9]+[kKmMgG]?)(..[0-9]+[kKmMgG]?)?").unwrap();
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

impl FromStr for SizeRange {
    type Err = String;

    fn from_str(s: &str) -> Result<SizeRange, Self::Err> {
        let captrs = match ARG_RE.captures(s) {
            None => return Ok(SizeRange(None, None)),
            Some(captrs) => captrs,
        };
        let x = captrs.get(1).map(|m| SizeRange::to_isize(m.as_str()));
        let y = captrs
            .get(2)
            .map(|m| SizeRange::to_isize(m.as_str().chars().skip(2).collect::<String>().as_str()));
        Ok(SizeRange(x.transpose()?, y.transpose()?))
    }
}

impl SizeRange {
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
            SizeRange(Some(x), Some(y)) => (x, y),
            SizeRange(Some(x), None) => return vec![x],
            SizeRange(None, Some(_)) => unreachable!(),
            SizeRange(None, None) => return vec![],
        };
        BLOCK_SIZES
            .clone()
            .iter()
            .skip_while(|x| **x < from)
            .take_while(|x| **x < till)
            .map(|x| *x)
            .collect()
    }

    fn get_datas(self) -> Vec<isize> {
        let (from, till) = match self {
            SizeRange(Some(x), Some(y)) => (x, y),
            SizeRange(Some(x), None) => return vec![x],
            SizeRange(None, Some(_)) => unreachable!(),
            SizeRange(None, None) => return vec![],
        };
        DATA_SIZES
            .clone()
            .iter()
            .skip_while(|x| **x < from)
            .take_while(|x| **x < till)
            .map(|x| *x)
            .collect()
    }
}

struct Stats {
    latencies: Vec<u64>,
}

impl Stats {
    fn new() -> Stats {
        Stats { latencies: vec![] }
    }

    fn join(&mut self, other: Stats) {
        self.latencies.extend_from_slice(&other.latencies);
    }
}

struct DiskioError(String);

impl fmt::Display for DiskioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<io::Error> for DiskioError {
    fn from(err: io::Error) -> DiskioError {
        DiskioError(err.description().to_string())
    }
}

impl From<time::SystemTimeError> for DiskioError {
    fn from(err: time::SystemTimeError) -> DiskioError {
        DiskioError(err.description().to_string())
    }
}

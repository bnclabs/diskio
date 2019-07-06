// TODO: replace sync_all with sync_data.

use std::{
    convert::TryInto,
    fs,
    io::{self, Write},
    path,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    thread, time,
};

use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
struct Opt {
    path: String,

    #[structopt(long = "block-size", default_value = "1024")]
    block_size: Isize,

    #[structopt(long = "data-size", default_value = "1073741824")]
    data_size: Isize,

    #[structopt(long = "threads", default_value = "1")]
    nthreads: isize,
}

struct Context {
    fd: Option<fs::File>,
    block: Vec<u8>,
    data_size: isize,
}

impl From<Opt> for Context {
    fn from(opt: Opt) -> Context {
        let mut block = Vec::with_capacity(opt.block_size.0 as usize);
        block.resize(block.capacity(), 0xAB);
        Context {
            fd: None,
            block,
            data_size: opt.data_size.0 / opt.nthreads,
        }
    }
}

impl Context {
    fn make_file(id: isize, dir: &path::Path) -> io::Result<fs::File> {
        let mut path = path::PathBuf::new();
        path.push(dir);
        fs::create_dir_all(path.as_path())?;
        path.push(format!("diskio-{}.data", id));
        fs::remove_file(path.as_path()).ok();

        println!("creating file: {} ..", path.to_str().unwrap());
        Ok(fs::OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path.as_path())?)
    }
}

static TOTAL: AtomicU64 = AtomicU64::new(0);

fn main() {
    let opt = Opt::from_args();
    let mut writers = vec![];
    let start_time = time::SystemTime::now();
    for i in 0..opt.nthreads {
        let mut ctxt: Context = opt.clone().into();
        let mut path = path::PathBuf::new();
        path.push(&opt.path);
        ctxt.fd = match Context::make_file(i, &path.as_path()) {
            Err(err) => {
                println!("invalid path: {:?}", err);
                return;
            }
            Ok(fd) => Some(fd),
        };
        writers.push(thread::spawn(move || writer_thread(ctxt)));
    }
    println!("writers: {}", writers.len());

    writers.into_iter().for_each(|w| w.join().unwrap());
    let total: usize = TOTAL.load(Ordering::Relaxed).try_into().unwrap();
    let elapsed = start_time.elapsed().expect("failed to compute elapsed");
    println!(
        "writen {} across {} threads in {:?}",
        humanize(total),
        opt.nthreads,
        elapsed
    );
}

fn writer_thread(mut ctxt: Context) {
    let mut fd = ctxt.fd.take().unwrap();
    while ctxt.data_size > 0 {
        match fd.write(&ctxt.block) {
            Ok(n) if n != ctxt.block.len() => {
                println!("partial write {}", n);
                return;
            }
            Err(err) => {
                println!("invalid write: {:?}", err);
                return;
            }
            _ => (),
        }
        fd.sync_all().unwrap();

        let n: isize = ctxt.block.len().try_into().unwrap();
        ctxt.data_size -= n;
    }
    let n: u64 = fd.metadata().unwrap().len().try_into().unwrap();
    TOTAL.fetch_add(n, Ordering::Relaxed);
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
struct Isize(isize);

impl FromStr for Isize {
    type Err = String;

    fn from_str(s: &str) -> Result<Isize, Self::Err> {
        if s.len() > 0 {
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
                    (s, 1024 * 1024 * 1024)
                }
                _ => {
                    let s: String = chs[..(chs.len() - 1)].iter().collect();
                    (s, 1)
                }
            };
            match s.parse::<isize>() {
                Err(err) => Err(format!("parse: {:?}", err)),
                Ok(n) => Ok(Isize(n * amp)),
            }
        } else {
            Ok(Isize(0))
        }
    }
}

// TODO: replace sync_all with sync_data.

use std::{
    convert::TryInto,
    fs,
    io::{self, Write},
    path,
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
struct Opt {
    path: String,

    #[structopt(long = "block-size", default_value = "1024")]
    block_size: usize,

    #[structopt(long = "data-size", default_value = "1073741824")]
    data_size: isize,

    #[structopt(long = "shards", default_value = "1")]
    nshards: isize,

    #[structopt(long = "threads", default_value = "1")]
    nthreads: isize,
}

struct Context {
    shards: Vec<fs::File>,
    block: Vec<u8>,
    data_size: isize,
}

impl From<Opt> for Context {
    fn from(opt: Opt) -> Context {
        let mut block = Vec::with_capacity(opt.block_size);
        block.resize(block.capacity(), 0xAB);
        Context {
            shards: vec![],
            block,
            data_size: opt.data_size / opt.nthreads,
        }
    }
}

impl Context {
    fn make_files(&mut self, nshards: isize, dir: &path::Path) -> io::Result<()> {
        for i in 0..nshards {
            let mut path = path::PathBuf::new();
            path.push(dir);
            fs::create_dir_all(path.as_path())?;
            path.push(format!("diskio-shard-{}.data", i));
            fs::remove_file(path.as_path())?;
            println!("creating file: {} ..", path.to_str().unwrap());
            let fd = fs::OpenOptions::new()
                .append(true)
                .create_new(true)
                .open(path.as_path())?;
            self.shards.push(fd);
        }
        Ok(())
    }
}

static TOTAL: AtomicU64 = AtomicU64::new(0);

fn main() {
    let opt = Opt::from_args();
    let mut writers = vec![];
    for i in 0..opt.nthreads {
        let mut ctxt: Context = opt.clone().into();
        let mut path = path::PathBuf::new();
        path.push(&opt.path);
        path.push(&format!("writer-{}", i));
        match ctxt.make_files(opt.nshards, &path.as_path()) {
            Err(err) => {
                println!("invalid path: {:?}", err);
                return;
            }
            _ => (),
        }
        writers.push(thread::spawn(move || writer_thread(ctxt)));
    }
    println!("writers: {}", writers.len());
    for writer in writers {
        writer.join().unwrap()
    }
    let total: usize = TOTAL.load(Ordering::Relaxed).try_into().unwrap();
    println!(
        "writen {} across {} files",
        humanize(total),
        opt.nthreads * opt.nshards
    );
}

fn writer_thread(mut ctxt: Context) {
    let mut shard = 0;
    while ctxt.data_size > 0 {
        let fd = &mut ctxt.shards[shard];
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
        shard = (shard + 1) % ctxt.shards.len();
    }
    for fd in ctxt.shards {
        let n: u64 = fd.metadata().unwrap().len().try_into().unwrap();
        TOTAL.fetch_add(n, Ordering::Relaxed);
    }
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

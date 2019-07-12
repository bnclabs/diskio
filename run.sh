# append data
#cargo run --release -- --appenders 1 --block-size 100K /tmp
#cargo run --release -- --appenders 1 --block-size 1M /tmp
#cargo run --release -- --appenders 2 --block-size 100K /tmp
#cargo run --release -- --appenders 2 --block-size 1M /tmp

# random write data
cargo run --release -- --appenders 0 --writers 1 --block-size 4K /tmp

#rm -rf /tmp/diskio*

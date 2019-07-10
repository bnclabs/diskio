cargo run --release -- --threads 1 --block-size 200..100K --data-size 1M..1G /tmp
cargo run --release -- --threads 1 --block-size 100K..1M --data-size 1G..10G /tmp
cargo run --release -- --threads 2 --block-size 200..100K --data-size 1M..1G /tmp
cargo run --release -- --threads 2 --block-size 100K..1M --data-size 1G..10G /tmp
cargo run --release -- --threads 4 --block-size 200..100K --data-size 1M..1G /tmp
cargo run --release -- --threads 4 --block-size 100K..1M --data-size 1G..10G /tmp
cargo run --release -- --threads 8 --block-size 200..100K --data-size 1M..1G /tmp
cargo run --release -- --threads 8 --block-size 100K..1M --data-size 1G..10G /tmp

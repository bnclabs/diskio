use plotters::prelude::*;
use std::path;

pub fn latency(path: path::PathBuf, latencies: Vec<u64>) -> Result<(), Box<dyn std::error::Error>> {
    let root = BitMapBackend::new(&path, (1024, 768)).into_drawing_area();
    root.fill(&White)?;

    let (xmin, xmax) = (0_u64, latencies.len() as u64);
    let (ymin, ymax) = (0, latencies.iter().max().cloned().unwrap_or(0));
    let mut scatter_ctx = ChartBuilder::on(&root)
        .x_label_area_size(40)
        .y_label_area_size(60)
        .build_ranged(xmin..xmax, ymin..ymax)?;
    scatter_ctx
        .configure_mesh()
        .disable_x_mesh()
        .disable_y_mesh()
        .draw()?;
    scatter_ctx.draw_series(
        latencies
            .iter()
            .enumerate()
            .map(|(i, l)| Circle::new((i as u64, *l), 2, Red.filled())),
    )?;

    Ok(())
}

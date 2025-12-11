use ::eframe::egui::{Color32, Response, Stroke, Ui};
use walkers::{MapMemory, Plugin, Position, Projector};
use walkers_extras::Place;

// Marker
pub struct Marker {
    position: Position,
}

impl Place for Marker {
    fn position(&self) -> Position {
        self.position
    }

    fn draw(&self, ui: &Ui, projector: &Projector) {
        let screen_position = projector.project(self.position).to_pos2();

        ui.painter().circle(
            screen_position,
            5.0,
            Color32::BLUE.gamma_multiply(0.8),
            Stroke::new(2., Color32::BLACK.gamma_multiply(0.8)),
        );
    }
}

// Point Plot Plugin
pub struct PointPlot {
    points: Vec<Position>,
}

impl PointPlot {
    pub fn new(points: Vec<Position>) -> Self {
        Self { points }
    }
}

impl Plugin for PointPlot {
    fn run(
        self: Box<Self>,
        ui: &mut Ui,
        _: &Response,
        projector: &Projector,
        _map_memory: &MapMemory,
    ) {
        self.points.iter().for_each(|pos| {
            Marker { position: *pos }.draw(ui, projector);
        });
    }
}

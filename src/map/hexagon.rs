use ::eframe::egui::{Color32, Response, Stroke, Ui};
use h3o::{CellIndex, LatLng};
use std::str::FromStr;
use walkers::{MapMemory, Plugin, Position, Projector};
use walkers_extras::Place;

// Uber H3 Cell
pub struct Hexagon {
    h3cell: String,
}

impl Hexagon {
    /// Returns the center position of the hexagon as a Position.
    pub fn position(&self) -> Option<Position> {
        let cell_index = CellIndex::from_str(&self.h3cell).ok()?;
        let latlng = LatLng::from(cell_index);
        Some(Position::new(latlng.lat(), latlng.lng()))
    }

    // /// Returns the boundary coordinates (lat, lng) of the hexagon.
    // pub fn boundary_coordinates(&self) -> Option<Vec<(f64, f64)>> {
    //     let cell_index = CellIndex::from_str(&self.h3cell).ok()?;

    //     let boundary = cell_index.boundary();
    //     let coords: Vec<(f64, f64)> = boundary
    //         .iter()
    //         .map(|latlng| (latlng.lat(), latlng.lng()))
    //         .collect();
    //     Some(coords)
    // }
}

impl Place for Hexagon {
    fn position(&self) -> Position {
        self.position().unwrap_or_else(|| Position::new(0.0, 0.0))
    }

    fn draw(&self, ui: &Ui, projector: &Projector) {
        let pos = self.position().unwrap_or_else(|| Position::new(0.0, 0.0));
        let screen_position = projector.project(pos).to_pos2();

        ui.painter().circle(
            screen_position,
            5.0,
            Color32::BLUE.gamma_multiply(0.8),
            Stroke::new(2., Color32::BLACK.gamma_multiply(0.8)),
        );
    }
}

// Hexagon Plot Plugin
pub struct HexagonPlot {
    h3cells: Vec<String>,
}

impl HexagonPlot {
    pub fn new(h3cells: Vec<String>) -> Self {
        Self { h3cells }
    }
}

impl Plugin for HexagonPlot {
    fn run(
        self: Box<Self>,
        ui: &mut Ui,
        _: &Response,
        projector: &Projector,
        _map_memory: &MapMemory,
    ) {
        self.h3cells.iter().for_each(|h3cell| {
            let hexagon = Hexagon {
                h3cell: h3cell.clone(),
            };
            hexagon.draw(ui, projector);
        });
    }
}

#![windows_subsystem = "windows"]
use eframe::egui::{
    self, CentralPanel, Color32, ComboBox, Context, CursorIcon, RichText, ScrollArea, TextStyle,
    TextWrapMode, Ui, ViewportBuilder, Window,
};
use egui::widgets::Label;
use egui_extras::{Column, TableBody, TableBuilder, TableRow};
use polars::prelude::*;
use rfd::FileDialog;
use std::env;
use std::path::PathBuf;
use walkers::{HttpTiles, MapMemory, Position, sources::OpenStreetMap};

mod ui {
    pub mod views;
}

use crate::ui::views::ViewTab;

mod plot {
    pub mod hexagon;
    pub mod marker;
}

use crate::plot::{hexagon::HexagonPlot, marker::PointPlot};

mod df {
    pub mod filter;
}

use crate::df::filter::FilterType;

struct Parqr {
    dataframe: Option<DataFrame>,
    original_dataframe: Option<DataFrame>,
    column_names: Vec<String>,
    files_to_load: Vec<PathBuf>,
    error_message: Option<String>,
    files_loaded: bool,

    selected_tab: ViewTab,

    filter_dialog_open: bool,
    selected_filter_column: Option<usize>,
    filter_text: String,
    filter_type: FilterType,

    sort_column: Option<usize>,
    sort_descending: bool,

    tiles: HttpTiles,
    map_memory: MapMemory,
    positions: Vec<Position>,
    h3cells: Vec<String>,
}

impl Parqr {
    fn new(files_to_load: Vec<PathBuf>, ctx: Context) -> Self {
        Self {
            dataframe: None,
            original_dataframe: None,
            column_names: Vec::new(),
            files_to_load,
            error_message: None,
            files_loaded: false,

            selected_tab: ViewTab::Map,

            filter_dialog_open: false,
            selected_filter_column: None,
            filter_text: String::new(),
            filter_type: FilterType::Equals,

            sort_column: None,
            sort_descending: false,

            tiles: HttpTiles::new(OpenStreetMap, ctx),
            map_memory: MapMemory::default(),
            positions: Vec::new(),
            h3cells: Vec::new(),
        }
    }

    fn load_parquet_data(&mut self, paths: Vec<PathBuf>) {
        self.dataframe = None;
        self.original_dataframe = None;
        self.column_names.clear();

        let pl_paths: Vec<PlPath> = paths
            .into_iter()
            .map(|pb| PlPath::Local(Arc::from(pb.into_boxed_path())))
            .collect();
        let scan_sources = ScanSources::Paths(Arc::from(pl_paths.into_boxed_slice()));

        match LazyFrame::scan_parquet_sources(scan_sources, ScanArgsParquet::default())
            .and_then(|lazy_frame| lazy_frame.collect())
        {
            Ok(df) => {
                let df_with_row_index = df.with_row_index("Row Index".into(), None).unwrap();
                self.column_names = df_with_row_index
                    .get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                self.original_dataframe = Some(df_with_row_index.clone());
                self.dataframe = Some(df_with_row_index);
                self.error_message = None;
                self.selected_filter_column = None;
                self.filter_text.clear();

                self.render_map_data();
            }
            Err(e) => {
                self.dataframe = None;
                self.original_dataframe = None;
                self.column_names.clear();
                self.error_message = Some(format!("Error processing Parquet files: {}", e));
            }
        }
    }

    fn process_pending_files(&mut self) {
        if !self.files_loaded && !self.files_to_load.is_empty() {
            self.load_parquet_data(self.files_to_load.clone());
            self.files_loaded = true;
        }
    }

    fn render_file_selector(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            if ui.button("Browse...").clicked() {
                self.handle_browse_button_click();
            }

            if self.files_to_load.is_empty() {
                ui.label("No Parquet files selected");
            } else if self.files_to_load.len() == 1 {
                ui.label(format!(
                    "Selected: {}",
                    self.files_to_load[0].file_name().unwrap().to_string_lossy()
                ));
            } else {
                ui.label(format!("Selected: {} files", self.files_to_load.len()));
            }

            ui.separator();

            ui.add_enabled_ui(self.dataframe.is_some(), |ui| {
                if ui.button("Filter").clicked() {
                    self.filter_dialog_open = true;
                }
            });
        });
    }

    fn handle_browse_button_click(&mut self) {
        if let Some(paths) = FileDialog::new()
            .add_filter("Parquet files", &["parquet"])
            .pick_files()
        {
            if paths.is_empty() {
                self.error_message =
                    Some("No files selected. Please select at least one Parquet file.".to_string());
            } else {
                self.files_to_load = paths;
                self.files_loaded = false;
                self.error_message = None;
            }
        }
    }

    fn render_error_message(&self, ui: &mut Ui) {
        if let Some(err_msg) = &self.error_message {
            ui.colored_label(Color32::RED, err_msg);
        }
    }

    fn apply_sort(&mut self) {
        if let (Some(df), Some(col_idx)) = (&self.dataframe, self.sort_column) {
            let col_name = &self.column_names[col_idx];

            match df.sort(
                vec![PlSmallStr::from(col_name)],
                SortMultipleOptions::new().with_order_descending(self.sort_descending),
            ) {
                Ok(sorted_df) => {
                    self.dataframe = Some(sorted_df);
                }
                Err(e) => {
                    self.error_message = Some(format!("Sort error: {}", e));
                }
            }
        }
    }

    fn apply_filter(&mut self) {
        if let (Some(original_df), Some(col_idx)) =
            (&self.original_dataframe, self.selected_filter_column)
        {
            let col_name = &self.column_names[col_idx];

            if self.filter_text.is_empty() {
                self.dataframe = Some(original_df.clone());
            } else {
                let lazy_df = original_df.clone().lazy();
                let filter_expr = match self.filter_type {
                    FilterType::Equals => col(col_name)
                        .cast(DataType::String)
                        .eq(lit(self.filter_text.clone())),
                    FilterType::Contains => col(col_name)
                        .cast(DataType::String)
                        .str()
                        .contains(lit(self.filter_text.clone()), false),
                };
                match lazy_df.filter(filter_expr).collect() {
                    Ok(filtered_df) => {
                        self.dataframe = Some(filtered_df);
                        self.error_message = None;
                    }
                    Err(e) => {
                        self.error_message = Some(format!("Filter error: {}", e));
                        self.dataframe = Some(original_df.clone());
                    }
                }
            }
        }

        self.render_map_data();
    }

    fn render_filter_dialog(&mut self, ctx: &Context) {
        let mut open = self.filter_dialog_open;
        Window::new("Filter")
            .auto_sized()
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.horizontal_top(|ui| {
                    ComboBox::from_id_salt("filter_column")
                        .selected_text(
                            self.selected_filter_column
                                .map(|idx| self.column_names[idx].as_str())
                                .unwrap_or("Select column"),
                        )
                        .show_ui(ui, |ui| {
                            let column_names = self.column_names.clone();
                            for (idx, col_name) in column_names.iter().enumerate() {
                                if ui
                                    .selectable_value(
                                        &mut self.selected_filter_column,
                                        Some(idx),
                                        col_name,
                                    )
                                    .clicked()
                                {
                                    self.apply_filter();
                                }
                            }
                        });

                    ui.add_enabled_ui(self.selected_filter_column.is_some(), |ui| {
                        ComboBox::from_id_salt("filter_type")
                            .selected_text(self.filter_type.to_string())
                            .show_ui(ui, |ui| {
                                if ui
                                    .selectable_value(
                                        &mut self.filter_type,
                                        FilterType::Equals,
                                        FilterType::Equals.to_string(),
                                    )
                                    .clicked()
                                {
                                    self.apply_filter();
                                }
                                if ui
                                    .selectable_value(
                                        &mut self.filter_type,
                                        FilterType::Contains,
                                        FilterType::Contains.to_string(),
                                    )
                                    .clicked()
                                {
                                    self.apply_filter();
                                }
                            });

                        let response = ui.text_edit_singleline(&mut self.filter_text);
                        if response.changed() {
                            self.apply_filter();
                        }
                        if ui.button("Clear Filter").clicked() {
                            self.selected_filter_column = None;
                            self.filter_text.clear();
                            self.error_message = None;
                            if let Some(original_df) = &self.original_dataframe {
                                self.dataframe = Some(original_df.clone());
                            }
                        }
                    });
                });
            });
        self.filter_dialog_open = open;
    }

    fn find_lat_lon_columns(&self) -> Option<(String, String)> {
        let lat_candidates = ["latitude", "lat"];
        let lon_candidates = ["longitude", "lon", "lng"];

        let mut lat_col = None;
        let mut lon_col = None;

        for col in &self.column_names {
            let col_lower = col.to_lowercase();
            if lat_candidates.iter().any(|&name| col_lower == name) {
                lat_col = Some(col.clone());
            }
            if lon_candidates.iter().any(|&name| col_lower == name) {
                lon_col = Some(col.clone());
            }
        }

        match (lat_col, lon_col) {
            (Some(lat), Some(lon)) => Some((lat, lon)),
            _ => None,
        }
    }

    fn extract_lat_lons(&self) -> Option<(Vec<f64>, Vec<f64>)> {
        let df = self.dataframe.as_ref()?;
        let (lat_col, lon_col) = self.find_lat_lon_columns()?;

        let lat_series = df.column(&lat_col).ok()?;
        let lon_series = df.column(&lon_col).ok()?;

        let lat_values: Vec<f64> = lat_series.f64().ok()?.into_no_null_iter().collect();
        let lon_values: Vec<f64> = lon_series.f64().ok()?.into_no_null_iter().collect();

        Some((lat_values, lon_values))
    }

    fn construct_lat_lon_positions(&mut self) {
        if let Some((latitudes, longitudes)) = self.extract_lat_lons() {
            self.positions = latitudes
                .iter()
                .zip(longitudes.iter())
                .map(|(&lat, &lon)| walkers::lat_lon(lat, lon))
                .collect();
        } else {
            self.positions.clear();
        }
    }

    fn find_h3cell_columns(&self) -> Option<String> {
        let h3_candidates = ["h3point", "h3cell", "h3index"];

        let mut h3_col = None;

        for col in &self.column_names {
            let col_lower = col.to_lowercase();
            if h3_candidates.iter().any(|&name| col_lower == name) {
                h3_col = Some(col.clone());
            }
        }

        h3_col
    }

    fn extract_h3cells(&self) -> Option<Vec<String>> {
        let df = self.dataframe.as_ref()?;
        let h3_col = self.find_h3cell_columns()?;

        let h3_series = df.column(&h3_col).ok()?;
        let h3_strings = h3_series.str().ok()?;
        let h3_values: Vec<String> = h3_strings
            .into_no_null_iter()
            .map(|s| s.to_string())
            .collect();

        Some(h3_values)
    }

    fn construct_h3_cells(&mut self) {
        if let Some(h3cells) = self.extract_h3cells() {
            self.h3cells = h3cells
        } else {
            self.h3cells.clear();
        }
    }

    fn render_map_data(&mut self) {
        self.construct_lat_lon_positions();
        // self.construct_h3_cells();
    }

    fn render_table_header(&mut self, header_row: &mut TableRow, column_names: &[String]) {
        for (i, col_name) in column_names.iter().enumerate() {
            header_row.col(|ui| {
                if ui
                    .add(
                        Label::new(
                            RichText::new(&format!(
                                "{} {}",
                                col_name,
                                if Some(i) == self.sort_column {
                                    if self.sort_descending { "⬇" } else { "⬆" }
                                } else {
                                    ""
                                }
                            ))
                            .strong(),
                        )
                        .wrap_mode(TextWrapMode::Extend),
                    )
                    .on_hover_cursor(CursorIcon::Default)
                    .clicked()
                {
                    if Some(i) == self.sort_column {
                        self.sort_descending = !self.sort_descending;
                    } else {
                        self.sort_column = Some(i);
                        self.sort_descending = false;
                    }
                    self.apply_sort();
                }
            });
        }
    }

    fn render_table_body(&self, body: TableBody, df: &DataFrame, column_names: &[String]) {
        let num_rows = df.height();
        body.rows(20.0, num_rows, |mut row| {
            for col_name in column_names {
                match df.column(col_name) {
                    Ok(column) => {
                        let cell_text = match column.get(row.index()) {
                            Ok(any_value) => any_value.to_string(),
                            Err(_) => "Error".to_string(),
                        };
                        row.col(|ui| {
                            ui.add(Label::new(&cell_text).wrap_mode(TextWrapMode::Extend));
                        });
                    }
                    Err(_) => {
                        row.col(|ui| {
                            ui.add(Label::new("Col?").wrap_mode(TextWrapMode::Extend));
                        });
                    }
                }
            }
        });
    }

    fn render_table(&mut self, ui: &mut Ui) {
        if let Some(df) = &self.dataframe.clone() {
            ScrollArea::horizontal()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    TableBuilder::new(ui)
                        .striped(true)
                        .resizable(true)
                        .columns(Column::auto().resizable(true), self.column_names.len() + 1)
                        .header(25.0, |mut header_row| {
                            self.render_table_header(&mut header_row, &self.column_names.clone());
                        })
                        .body(|body| {
                            self.render_table_body(body, df, &self.column_names);
                        });
                });
        }
    }
}

impl eframe::App for Parqr {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        let font_size = 18.;
        ctx.style_mut(|style| {
            style.text_styles.get_mut(&TextStyle::Body).unwrap().size = font_size;
            style.text_styles.get_mut(&TextStyle::Button).unwrap().size = font_size;
        });

        self.process_pending_files();

        self.render_filter_dialog(ctx);

        CentralPanel::default().show(ctx, |ui| {
            self.render_file_selector(ui);
            ui.separator();
            self.render_error_message(ui);

            ui.horizontal(|ui| {
                if ui
                    .selectable_label(matches!(self.selected_tab, ViewTab::Map), "Map")
                    .clicked()
                {
                    self.selected_tab = ViewTab::Map;
                }
                if ui
                    .selectable_label(matches!(self.selected_tab, ViewTab::Table), "Table")
                    .clicked()
                {
                    self.selected_tab = ViewTab::Table;
                }
            });
            ui.separator();

            match self.selected_tab {
                ViewTab::Map => {
                    let position = self
                        .positions
                        .last()
                        .cloned()
                        .unwrap_or_else(|| walkers::lon_lat(0.0, 52.0));

                    let map =
                        walkers::Map::new(Some(&mut self.tiles), &mut self.map_memory, position)
                            .with_plugin(PointPlot::new(self.positions.clone()))
                            .with_plugin(HexagonPlot::new(self.h3cells.clone()));
                    ui.add(map);
                }
                ViewTab::Table => {
                    self.render_table(ui);
                }
            }
        });
    }
}

fn main() -> Result<(), eframe::Error> {
    let paths: Vec<PathBuf> = env::args().skip(1).map(PathBuf::from).collect();

    let icon = include_bytes!("../assets/parqr.png");
    let image = image::load_from_memory(icon)
        .expect("Failed to open icon path")
        .to_rgba8();
    let (icon_width, icon_height) = image.dimensions();

    let options = eframe::NativeOptions {
        viewport: ViewportBuilder::default()
            .with_inner_size([1200.0, 800.0])
            .with_icon(egui::IconData {
                rgba: image.into_raw(),
                width: icon_width,
                height: icon_height,
            }),
        ..Default::default()
    };
    eframe::run_native(
        "Mapr - Parquet Mapper",
        options,
        Box::new(|cc| {
            let ctx = cc.egui_ctx.clone();
            Ok(Box::new(Parqr::new(paths, ctx)))
        }),
    )
}

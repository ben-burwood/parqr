#![windows_subsystem = "windows"]
use eframe::egui::{
    self, CentralPanel, Color32, Context, CursorIcon, RichText, ScrollArea, TextStyle,
    TextWrapMode, Ui, ViewportBuilder,
};
use egui::widgets::Label;
use egui_extras::{Column, TableBuilder, TableRow};
use polars::prelude::*;
use polars::prelude::CsvWriter;
use polars::prelude::ParquetWriter;
use rfd::FileDialog;
use std::env;
use std::path::PathBuf;
use walkers::{HttpTiles, MapMemory, Position, sources::OpenStreetMap};

mod ui {
    pub mod views;
}
use crate::ui::views::ViewTab;

mod df {
    pub mod filter;
    pub mod sort;
    pub mod export;
}
use crate::df::export::ExportFileType;

use crate::df::{filter::FilterType, sort::SortCondition};

mod table {
    pub mod table;
}
use crate::table::table::render_table_body;

mod map {
    pub mod hexagon;
    pub mod marker;
}
use crate::map::{hexagon::HexagonPlot, marker::PointPlot};


struct Parqr {
    dataframe: Option<DataFrame>,
    original_dataframe: Option<DataFrame>,
    column_names: Vec<String>,
    files_to_load: Vec<PathBuf>,
    error_message: Option<String>,
    files_loaded: bool,

    selected_tab: ViewTab,

    filter_dialog_open: bool,
    filter_conditions: Vec<df::filter::FilterCondition>,

    sort_condition: Option<SortCondition>,

    tiles: HttpTiles,
    map_memory: MapMemory,
    positions: Vec<Position>,
    h3cells: Vec<String>,

    export_file_path: Option<PathBuf>,
    export_file_type: ExportFileType,
    export_result: Option<String>,
    export_selected_columns: Option<Vec<String>>,
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

            selected_tab: ViewTab::Table,

            filter_dialog_open: false,
            filter_conditions: Vec::new(),

            sort_condition: None,

            tiles: HttpTiles::new(OpenStreetMap, ctx),
            map_memory: MapMemory::default(),
            positions: Vec::new(),
            h3cells: Vec::new(),

            export_file_path: None,
            export_file_type: ExportFileType::Csv,
            export_result: None,
            export_selected_columns: None,
        }
    }

    fn load_parquet_data(&mut self, paths: Vec<PathBuf>) {
        self.dataframe = None;
        self.original_dataframe = None;
        self.column_names.clear();
        // Reset export columns selection when new data is loaded
        self.export_selected_columns = None;

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
                self.filter_conditions = Vec::new();

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
        if let (Some(df), Some(sort_cond)) = (&self.dataframe, &self.sort_condition) {
            let column = PlSmallStr::from(&sort_cond.column_name);

            match df.sort(
                vec![column],
                SortMultipleOptions::new().with_order_descending(!sort_cond.ascending),
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
        if let Some(original_df) = &self.original_dataframe {
            if self.filter_conditions.is_empty() {
                self.dataframe = Some(original_df.clone());
                self.error_message = None;
            } else {
                match df::filter::filter_dataframe(original_df, &self.filter_conditions) {
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

    fn render_filter_dialog(&mut self, ui: &mut egui::Ui) {
        let mut apply_filter = false;
        let mut add_filter = false;
        let mut remove_indices = Vec::new();
        let filter_len = self.filter_conditions.len();

        if self.filter_dialog_open {
            egui::Window::new("Filter")
                .open(&mut self.filter_dialog_open)
                .auto_sized()
                .collapsible(false)
                .show(ui.ctx(), |ui| {
                    ui.vertical(|ui| {
                        for (i, filter) in self.filter_conditions.iter_mut().enumerate() {
                            ui.horizontal(|ui| {
                                // Column dropdown
                                let col_changed =
                                    egui::ComboBox::from_id_salt(format!("filter_column_{}", i))
                                        .selected_text(&filter.column_name)
                                        .show_ui(ui, |ui| {
                                            let mut changed = false;
                                            for col in &self.column_names {
                                                if ui
                                                    .selectable_value(
                                                        &mut filter.column_name,
                                                        col.clone(),
                                                        col,
                                                    )
                                                    .changed()
                                                {
                                                    changed = true;
                                                }
                                            }
                                            changed
                                        })
                                        .inner
                                        .unwrap_or(false);

                                // Filter type dropdown
                                let type_changed =
                                    egui::ComboBox::from_id_salt(format!("filter_type_{}", i))
                                        .selected_text(format!("{:?}", filter.filter_type))
                                        .show_ui(ui, |ui| {
                                            let mut changed = false;
                                            if ui
                                                .selectable_value(
                                                    &mut filter.filter_type,
                                                    FilterType::Contains,
                                                    FilterType::Contains.to_string(),
                                                )
                                                .changed()
                                            {
                                                changed = true;
                                            }
                                            if ui
                                                .selectable_value(
                                                    &mut filter.filter_type,
                                                    FilterType::Equals,
                                                    FilterType::Equals.to_string(),
                                                )
                                                .changed()
                                            {
                                                changed = true;
                                            }
                                            changed
                                        })
                                        .inner
                                        .unwrap_or(false);

                                let value_changed =
                                    ui.text_edit_singleline(&mut filter.filter_value).changed();

                                if filter_len > 1 {
                                    if ui.button("Remove").clicked() {
                                        remove_indices.push(i);
                                    }
                                }

                                // If any field changed, trigger live filtering
                                if col_changed || type_changed || value_changed {
                                    apply_filter = true;
                                }
                            });
                        }

                        if ui.button("Add Filter").clicked() {
                            add_filter = true;
                        }
                    });
                });
        }

        for &i in remove_indices.iter().rev() {
            self.filter_conditions.remove(i);
        }

        if add_filter {
            self.filter_conditions.push(df::filter::FilterCondition {
                column_name: self.column_names.get(0).cloned().unwrap_or_default(),
                filter_type: df::filter::FilterType::Equals,
                filter_value: String::new(),
            });
            apply_filter = true;
        }

        if self.filter_conditions.is_empty() {
            self.filter_conditions.push(df::filter::FilterCondition {
                column_name: self.column_names.get(0).cloned().unwrap_or_default(),
                filter_type: df::filter::FilterType::Contains,
                filter_value: String::new(),
            });
            apply_filter = true;
        }

        if apply_filter {
            self.apply_filter();
        }
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
        for col_name in column_names {
            header_row.col(|ui| {
                // Determine sort indicator
                let sort_indicator = if let Some(sort_cond) = &self.sort_condition {
                    if &sort_cond.column_name == col_name {
                        if sort_cond.ascending { "⬆" } else { "⬇" }
                    } else {
                        ""
                    }
                } else {
                    ""
                };

                if ui
                    .add(
                        Label::new(
                            RichText::new(format!("{} {}", col_name, sort_indicator)).strong(),
                        )
                        .wrap_mode(TextWrapMode::Extend),
                    )
                    .on_hover_cursor(CursorIcon::Default)
                    .clicked()
                {
                    // Toggle sort or set new sort condition
                    if let Some(sort_cond) = &mut self.sort_condition {
                        if sort_cond.column_name == *col_name {
                            sort_cond.ascending = !sort_cond.ascending;
                        } else {
                            self.sort_condition = Some(SortCondition {
                                column_name: col_name.clone(),
                                ascending: true,
                            });
                        }
                    } else {
                        self.sort_condition = Some(SortCondition {
                            column_name: col_name.clone(),
                            ascending: true,
                        });
                    }
                    self.apply_sort();
                }
            });
        }
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
                            render_table_body(body, df, &self.column_names);
                        });
                });
        }
    }

    fn render_export_pane(&mut self, ui: &mut Ui) {
        use crate::df::export::ExportFileType;
        ui.vertical(|ui| {
            // File type dropdown
            ui.horizontal(|ui| {
                ui.label("File type:");
                egui::ComboBox::from_id_salt("export_file_type")
                    .selected_text(match self.export_file_type {
                        ExportFileType::Csv => "CSV",
                        ExportFileType::Parquet => "Parquet",
                    })
                    .show_ui(ui, |ui| {
                        if ui
                            .selectable_value(&mut self.export_file_type, ExportFileType::Csv, "CSV")
                            .changed()
                        {}
                        if ui
                            .selectable_value(&mut self.export_file_type, ExportFileType::Parquet, "Parquet")
                            .changed()
                        {}
                    });
            });

            // File selector for export path
            ui.horizontal(|ui| {
                let file_label = if let Some(path) = &self.export_file_path {
                    path.file_name()
                        .map(|n| n.to_string_lossy().to_string())
                        .unwrap_or_else(|| "Choose export file...".to_string())
                } else {
                    "Choose export file...".to_string()
                };
                if ui.button("Browse...").clicked() {
                    let mut dialog = FileDialog::new();
                    match self.export_file_type {
                        ExportFileType::Csv => dialog = dialog.add_filter("CSV", &["csv"]),
                        ExportFileType::Parquet => dialog = dialog.add_filter("Parquet", &["parquet"]),
                    }
                    if let Some(path) = dialog.save_file() {
                        self.export_file_path = Some(path);
                    }
                }
                ui.label(file_label);
            });

            // Column multi-select
            ui.separator();
            let mut all_columns: Vec<(String, DataType)> = Vec::new();
            if let Some(df) = &self.dataframe {
                all_columns = df
                    .get_columns()
                    .iter()
                    .map(|s| (s.name().to_string(), s.dtype().clone()))
                    .collect();
            }
            if self.export_selected_columns.is_none() && !all_columns.is_empty() {
                self.export_selected_columns = Some(all_columns.iter().map(|(n, _)| n.clone()).collect());
            }

            ui.label("Columns to export:");
            if !all_columns.is_empty() {
                let selected = self.export_selected_columns.get_or_insert_with(|| all_columns.iter().map(|(n, _)| n.clone()).collect());
                ui.horizontal(|ui| {
                    if ui.button("Select All").clicked() {
                        *selected = all_columns.iter().map(|(n, _)| n.clone()).collect();
                    }
                    if ui.button("Deselect All").clicked() {
                        selected.clear();
                    }
                });
                egui::ScrollArea::vertical().max_height(120.0).show(ui, |ui| {
                    for (col, dtype) in &all_columns {
                        let mut checked = selected.contains(col);
                        let label = format!("{col} ({dtype:?})");
                        if ui.checkbox(&mut checked, label).changed() {
                            if checked {
                                if !selected.contains(col) {
                                    selected.push(col.clone());
                                }
                            } else {
                                selected.retain(|c| c != col);
                            }
                        }
                    }
                });
            }

            ui.separator();
            let can_export = self.dataframe.is_some()
                && self.export_file_path.is_some()
                && self.export_selected_columns.as_ref().map_or(false, |v| !v.is_empty());
            if ui
                .add_enabled(can_export, egui::Button::new("Export"))
                .clicked()
            {
                self.export_result = Some(
                    match (&self.dataframe, &self.export_file_path, &self.export_file_type, &self.export_selected_columns) {
                        (Some(df), Some(path), ExportFileType::Csv, Some(cols)) => {
                            let mut file = match std::fs::File::create(path) {
                                Ok(f) => f,
                                Err(e) => return self.export_result = Some(format!("File error: {e}")),
                            };
                            let mut df = if cols.len() == df.width() {
                                df.clone()
                            } else {
                                match df.select(cols) {
                                    Ok(sub) => sub,
                                    Err(e) => {
                                        self.export_result = Some(format!("Column select error: {e}"));
                                        return;
                                    }
                                }
                            };
                            let mut writer = CsvWriter::new(&mut file);
                            match writer.finish(&mut df) {
                                Ok(_) => format!("Exported to CSV: {}", path.display()),
                                Err(e) => format!("CSV export error: {e}"),
                            }
                        }
                        (Some(df), Some(path), ExportFileType::Parquet, Some(cols)) => {
                            let mut file = match std::fs::File::create(path) {
                                Ok(f) => f,
                                Err(e) => return self.export_result = Some(format!("File error: {e}")),
                            };
                            let mut df = if cols.len() == df.width() {
                                df.clone()
                            } else {
                                match df.select(cols) {
                                    Ok(sub) => sub,
                                    Err(e) => {
                                        self.export_result = Some(format!("Column select error: {e}"));
                                        return;
                                    }
                                }
                            };
                            let writer = ParquetWriter::new(&mut file);
                            match writer.finish(&mut df) {
                                Ok(_) => format!("Exported to Parquet: {}", path.display()),
                                Err(e) => format!("Parquet export error: {e}"),
                            }
                        }
                        _ => "No DataFrame, file path, or columns selected.".to_string(),
                    }
                );
            }

            if let Some(msg) = &self.export_result {
                ui.label(msg);
            }
        });
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

        CentralPanel::default().show(ctx, |ui| {
            self.render_file_selector(ui);
            self.render_filter_dialog(ui);

            ui.separator();
            self.render_error_message(ui);

            ui.horizontal(|ui| {
                if ui
                    .selectable_label(matches!(self.selected_tab, ViewTab::Table), "Table")
                    .clicked()
                {
                    self.selected_tab = ViewTab::Table;
                }
                if ui
                    .selectable_label(matches!(self.selected_tab, ViewTab::Map), "Map")
                    .clicked()
                {
                    self.selected_tab = ViewTab::Map;
                }
                if ui
                    .selectable_label(matches!(self.selected_tab, ViewTab::Export), "Export")
                    .clicked()
                {
                    self.selected_tab = ViewTab::Export;
                }
            });
            ui.separator();

            match self.selected_tab {
                ViewTab::Table => {
                    self.render_table(ui);
                }
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
                ViewTab::Export => {
                    self.render_export_pane(ui);
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
        "Parqr - Parquet Viewer",
        options,
        Box::new(|cc| {
            let ctx = cc.egui_ctx.clone();
            Ok(Box::new(Parqr::new(paths, ctx)))
        }),
    )
}

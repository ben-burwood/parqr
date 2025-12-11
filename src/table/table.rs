use eframe::egui::{Label, TextWrapMode};
use egui_extras::TableBody;
use polars::frame::DataFrame;

pub fn render_table_body(body: TableBody, df: &DataFrame, column_names: &[String]) {
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

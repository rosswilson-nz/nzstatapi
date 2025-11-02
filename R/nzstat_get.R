nzstat_get <- function(
  dataflow_id,
  dimensions = list(),
  max_tries = 10L,
  base_url = get_base_url(),
  api_key = get_api_key()
) {
  dataflows <- nzstat_get_dataflows()
  dataflow <- dataflows[dataflows$DataflowID == dataflow_id, ]
  datastructure <- nzstat_get_datastructure(dataflow_id, max_tries = max_tries)

  flowref <- paste(
    dataflow$AgencyID,
    dataflow$DataflowID,
    dataflow$Version,
    sep = ","
  )
  if (length(dimensions) == 0) {
    key <- "ALL"
  } else {
    key <- purrr::map(datastructure$dimension_id, \(dim) {
      paste(dimensions[[dim]] %||% "", collapse = "+")
    }) |>
      paste(collapse = ".")
  }

  req <- httr2::request(base_url) |>
    httr2::req_headers_redacted(
      "Ocp-Apim-Subscription-Key" = api_key
    ) |>
    httr2::req_headers(
      "Accept" = "application/xml",
      "user-agent" = make_user_agent()
    ) |>
    httr2::req_url_path_append("data", flowref, key) |>
    httr2::req_url_query(format = "csvfilewithlabels") |>
    httr2::req_retry(max_tries)

  resp <- req |> httr2::req_perform()

  httr2::resp_body_string(resp) |>
    I() |>
    readr::read_csv(show_col_types = FALSE)
}

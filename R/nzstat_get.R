nzstat_get <- function(
  dataflow_id,
  dimensions = list(),
  max_tries = 10L,
  base_url = get_base_url(),
  api_key = get_api_key()
) {
  # Validate inputs ----
  if (!rlang::is_string(dataflow_id)) {
    cli::cli_abort(c(
      "{.var dataflow_id} must be a string",
      x = "You've supplied {.type {dataflow_id}}"
    ))
  }
  if (!(rlang::is_bare_list(dimensions) && rlang::is_named(dimensions))) {
    cli::cli_abort(c(
      "{.var dimensions} must be a named list",
      x = "You've supplied {.type {dimensions}}"
    ))
  }
  if (!purrr::every(dimensions, rlang::is_bare_character)) {
    wrong_dimensions <- which(
      !purrr::map_lgl(dimensions, rlang::is_bare_character)
    )
    cli::cli_abort(c(
      "Each element of {.var dimensions} must be a character vector",
      x = "Element {.val {wrong_dimensions[[1]]}} is {.type {dimensions[[wrong_dimensions[[1]]]]}}"
    ))
  }
  if (!rlang::is_bare_integerish(max_tries, 1)) {
    cli::cli_abort(c(
      "{.var max_tries} must be an integer",
      x = "You've supplied {.type {max_tries}}"
    ))
  }
  if (!rlang::is_string(base_url)) {
    cli::cli_abort(c(
      "{.var base_url} must be a string",
      x = "You've supplied {.type {base_url}}"
    ))
  }
  if (!rlang::is_string(api_key)) {
    cli::cli_abort(c(
      "{.var api_key} must be a string",
      x = "You've supplied {.type {api_key}}"
    ))
  }

  # Get dataflows and datastructure ----
  dataflows <- get_dataflows(max_tries, base_url, api_key)
  if (!(dataflow_id %in% dataflows$DataflowID)) {
    cli::cli_abort(c(
      "{.var dataflow_id}={.val {dataflow_id}} not found",
      i = "Please check {.fn nzstat_get_dataflows} to select an available dataflow"
    ))
  }
  dataflow <- dataflows[dataflows$DataflowID == dataflow_id, ]
  datastructure <- get_datastructures(dataflow, max_tries, base_url, api_key)
  if (!all(names(dimensions) %in% datastructure$DimensionID)) {
    wrong_ids <- names(dimensions)[
      !(names(dimensions) %in% datastructure$DimensionID)
    ]
    cli::cli_abort(c(
      "Unknown {.var dimensions} {.val {wrong_ids}}",
      i = "Please check {.fn nzstat_get_datastructures} to confirm available dimensions"
    ))
  }

  # Construct request ----
  flowref <- paste(
    dataflow$AgencyID,
    dataflow$DataflowID,
    dataflow$Version,
    sep = ","
  )
  if (length(dimensions) == 0) {
    key <- "ALL"
  } else {
    key <- purrr::map(datastructure$DimensionID, \(dim) {
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

  # Perform request ----
  resp <- req |> httr2::req_perform()

  httr2::resp_body_string(resp) |>
    I() |>
    readr::read_csv(show_col_types = FALSE)
}

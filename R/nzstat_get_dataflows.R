#' Get dataflow definitions for all available Aotearoa Data Explorer datasets
#'
#' Retrieves, formats, and optionally filters all available dataflow definitions from

nzstat_get_dataflows <- function(
  search = NULL,
  max_tries = 10L,
  base_url = get_base_url(),
  api_key = get_api_key()
) {
  # Validate inputs ----
  if (!(is.null(search) | rlang::is_string(search))) {
    cli::cli_abort(c(
      "{.var search} must be {.type {NULL}} or a string",
      x = "You've supplied {.type {search}}"
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

  # Perform request ----
  tbl <- get_dataflows(max_tries, base_url, api_key)

  if (!is.null(search)) {
    tbl <- tbl[grepl(search, tbl$Name, ignore.case = TRUE), ]
  }

  tbl[, c("Name", "DataflowID")]
}

get_dataflows <- function(max_tries, base_url, api_key) {
  # Construct request ----
  ref <- c("dataflow", "STATSNZ", "all")
  req <- httr2::request(base_url) |>
    httr2::req_headers_redacted("Ocp-Apim-Subscription-Key" = api_key) |>
    httr2::req_headers(
      "user-agent" = make_user_agent()
    ) |>
    httr2::req_url_path_append(ref) |>
    httr2::req_url_query(detail = "full") |>
    httr2::req_retry(max_tries) |>
    httr2::req_cache(tempdir())

  # Perform request ----
  resp <- req |>
    httr2::req_perform() |>
    httr2::resp_body_json()

  # Extract request data to tibble ----
  tbl <- purrr::map(resp$references, extract_dataflow_catalogue) |>
    purrr::list_rbind()
}

extract_dataflow_catalogue <- function(dataflow) {
  tibble::tibble(
    Name = dataflow$name,
    DataflowID = dataflow$id,
    AgencyID = dataflow$agencyID,
    Version = dataflow$version
  )
}

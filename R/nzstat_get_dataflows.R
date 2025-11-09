#' Get dataflow definitions for all available Aotearoa Data Explorer datasets
#'
#' Retrieves, formats, and optionally filters all available dataflow definitions
#'     from the Aotearoa Data Explorer API.
#'
#' @param search Optional search string. A regex pattern passed to [grepl()].
#' @param max_tries `FALSE`, or integer giving maximum retry attempts. Passed to
#'     [httr2::req_retry()].
#' @param base_url The base URL to the API. If not set, uses the
#'     `getOption("NZSTAT_BASE_URL")`, or defaults to
#'     `"https://api.data.stats.govt.nz/rest/"`
#' @param api_key The API key to authenticate with the server. If not specified,
#'     will look in environment variable `"NZSTAT_API_KEY"`, and throw an error
#'     if that is not found.
#'
#' @returns A tibble, with columns `Name`: the name/description of the dataflow;
#'     and `DataflowID`: the ID used to identify the dataflow in requests.
#'
#' @export
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
  if (!(isFALSE(max_tries) || rlang::is_bare_integerish(max_tries, 1))) {
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
  if (is.null(the$dataflows)) {
    the$dataflows <- get_dataflows(
      max_tries = 10L,
      base_url = get_base_url(),
      api_key = api_key
    )
  }
  tbl <- the$dataflows

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
    httr2::req_url_query(detail = "full")
  if (max_tries > 1) {
    req <- req |> httr2::req_retry(max_tries)
  }

  # Perform request ----
  resp <- req |>
    httr2::req_perform() |>
    httr2::resp_body_xml() |>
    xml2::as_list()

  # Extract request data to tibble ----
  purrr::map(
    resp$Structure$Structures$Dataflows,
    extract_dataflow_catalogue
  ) |>
    purrr::list_rbind()
}

extract_dataflow_catalogue <- function(dataflow) {
  tibble::tibble(
    Name = dataflow$Name[[1]],
    DataflowID = attr(dataflow, "id"),
    AgencyID = attr(dataflow, "agencyID"),
    Version = attr(dataflow, "version"),
  )
}

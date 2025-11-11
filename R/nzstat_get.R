#' Get data from an Aotearoa Data Explorer dataflow
#'
#' Retrieves data from an Aotearoa Data Explorer dataflow.
#'
#' @param dataflow_id String. The DataflowID of a dataflow in the API.
#' @param dimensions Named list specifying codes to include for each dimension
#'     of the dataset. Each element should be a character string listing the
#'     codes to include for the corresponding dimension. All codes will be
#'     included for omitted dimensions, so only those dimensions you wish to
#'     subset need be listed. Each name should correspond to one of the elements
#'     in the `Name` column of `nzstat_get_datastructure(dataflow_id)`.
#' @param max_tries Integer; maximum retry attempts. Passed to
#'     [httr2::req_retry()].
#' @param base_url The base URL to the API. If not set, uses the
#'     `getOption("NZSTAT_BASE_URL")`, or defaults to
#'     `"https://api.data.stats.govt.nz/rest/"`
#' @param api_key The API key to authenticate with the server. If not specified,
#'     will look in environment variable `"NZSTAT_API_KEY"`, and throw an error
#'     if that is not found.
#'
#' @returns A tibble, with columns `DimensionID`: the ID used to identify the
#'     data dimension in requests; `CodeID`: the ID used to identify code values
#'     for the corresponding dimension; and `Name`: the name/description of the
#'     code.
#'
#' @export
nzstat_get <- function(
  dataflow_id,
  dimensions = list(),
  max_tries = 10L,
  cache = TRUE,
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
  if (is.null(the$dataflows)) {
    the$dataflows <- get_dataflows(max_tries, base_url, api_key)
  }
  if (!(dataflow_id %in% the$dataflows$DataflowID)) {
    cli::cli_abort(c(
      "{.var dataflow_id}={.val {dataflow_id}} not found",
      i = "Please check {.fn nzstat_get_dataflows} to select an available dataflow"
    ))
  }
  dataflow <- the$dataflows[the$dataflows$DataflowID == dataflow_id, ]

  if (is.null(the$datastructures[[dataflow_id]])) {
    the$datastructures[[dataflow_id]] <- get_datastructures(
      dataflow,
      max_tries,
      base_url,
      api_key
    )
  }
  if (!all(names(dimensions) %in% the$datastructures[[dataflow_id]]$Name)) {
    wrong_ids <- names(dimensions)[
      !(names(dimensions) %in% the$datastructures[[dataflow_id]]$Name)
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
    key <- purrr::map(the$datastructures[[dataflow_id]]$Name, \(dim) {
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
    httr2::req_url_query(format = "csvfilewithlabels")
  if (max_tries > 1) {
    req <- req |>
      httr2::req_retry(max_tries)
  }
  if (cache) {
    req <- req |> httr2::req_cache(tempdir())
  }

  # Perform request ----
  resp <- req |> httr2::req_perform()

  httr2::resp_body_string(resp) |>
    I() |>
    readr::read_csv(show_col_types = FALSE)
}

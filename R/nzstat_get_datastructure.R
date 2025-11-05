#' Get the datastructure definition for an Aotearoa Data Explorer dataflow
#'
#' Retrieves and format the datastructure definition for an Aotearoa Data
#'     Explorer dataflow.
#'
#' @param dataflow_id String. The DataflowID of a dataflow in the API.
#' @param max_tries Integer; maximum retry attempts. Passed to
#'     [httr2::req_retry()].
#' @param base_url The base URL to the API. If not set, uses the
#'     `getOption("NZSTAT_BASE_URL")`, or defaults to
#'     `"https://api.data.stats.govt.nz/rest/"`
#' @param api_key The API key to authenticate with the server. If not specified,
#'     will look in environment variable `"NZSTAT_API_KEY"`, and throw an error
#'     if that is not found.
#'
#' @returns A tibble, with column `DimensionID`: the ID used to identify the
#'     data dimension in requests.
#'
#' @export
nzstat_get_datastructure <- function(
  dataflow_id,
  max_tries = 10L,
  base_url = get_base_url(),
  api_key = get_api_key()
) {
  # Validate inputs ----
  if (!rlang::is_string(dataflow_id)) {
    cli::cli_abort(c(
      "{.var dataflow_id} must be {.type {NULL}} or a string",
      x = "You've supplied {.type {dataflow_id}}"
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

  # Get dataflows
  dataflows <- get_dataflows(max_tries, base_url, api_key)
  if (!(dataflow_id %in% dataflows$DataflowID)) {
    cli::cli_abort(c(
      "{.var dataflow_id}={.val {dataflow_id}} not found",
      i = "Please check {.fn nzstat_get_dataflows} to select an available dataflow"
    ))
  }
  dataflow <- as.list(dataflows[dataflows$DataflowID == dataflow_id, ])

  # Perform request ----
  tbl <- get_datastructures(dataflow, max_tries, base_url, api_key)

  tbl[, c("DimensionID")]
}

get_datastructures <- function(dataflow, max_tries, base_url, api_key) {
  # Construct request ----
  ref <- c(
    "dataflow",
    dataflow$AgencyID,
    dataflow$DataflowID,
    dataflow$Version,
    "all"
  )

  req <- httr2::request(base_url) |>
    httr2::req_headers_redacted(
      "Ocp-Apim-Subscription-Key" = api_key
    ) |>
    httr2::req_headers(
      "Accept" = "application/xml",
      "user-agent" = make_user_agent()
    ) |>
    httr2::req_url_path_append(ref) |>
    httr2::req_url_query(references = "datastructure") |>
    httr2::req_retry(max_tries) |>
    httr2::req_cache(tempdir())

  resp <- req |> httr2::req_perform()

  resp_list <- resp |> httr2::resp_body_xml() |> xml2::as_list()

  dsd <- purrr::map(
    resp_list$Structure$Structures$DataStructures[[
      1
    ]]$DataStructureComponents$DimensionList,
    \(dimension) extract_datastructure_dimension(dimension, dataflow_id)
  ) |>
    purrr::list_rbind()

  dsd[order(dsd$Position), ]
}

extract_datastructure_dimension <- function(dimension, dataflow_id) {
  tibble::tibble(
    DimensionID = attr(dimension, "id"),
    CodelistID = attr(dimension$LocalRepresentation$Enumeration$Ref, "id"),
    Position = as.integer(attr(dimension, "position"))
  )
}

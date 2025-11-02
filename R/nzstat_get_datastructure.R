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
  get_datastructures(dataflow, max_tries, base_url, api_key)
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

  dsd[order(dsd$position), ]
}

extract_datastructure_dimension <- function(dimension, dataflow_id) {
  tibble::tibble(
    dimension_id = attr(dimension, "id"),
    position = as.integer(attr(dimension, "position"))
  )
}

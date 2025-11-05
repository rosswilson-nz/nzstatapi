nzstat_get_codelists <- function(
  dataflow_id,
  dimension_ids,
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
  if (!(missing(dimension_ids) || rlang::is_bare_character(dimension_ids))) {
    cli::cli_abort(c(
      "{.var dimension_ids} must be a character vector",
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

  # Get dataflows & datastructures ----
  dataflows <- get_dataflows(max_tries, base_url, api_key)
  if (!(dataflow_id %in% dataflows$DataflowID)) {
    cli::cli_abort(c(
      "{.var dataflow_id}={.val {dataflow_id}} not found",
      i = "Please check {.fn nzstat_get_dataflows} to select an available dataflow"
    ))
  }
  dataflow <- as.list(dataflows[dataflows$DataflowID == dataflow_id, ])
  datastructure <- get_datastructures(dataflow, max_tries, base_url, api_key)

  # Perform request ----
  codelists <- get_codelists(dataflow_id, max_tries, base_url, api_key)

  if (missing(dimension_ids)) {
    dimension_ids <- datastructure$DimensionID
  }
  codelist_ids <- datastructure[
    datastructure$DimensionID %in% dimension_ids,
  ]$CodelistID

  purrr::map2(dimension_ids, codelist_ids, \(dim, cl) {
    out <- codelists[codelists$CodelistID %in% cl, ]
    out$DimensionID <- dim
    out[, c("DimensionID", "CodeID", "Name")]
  }) |>
    purrr::list_rbind()
}

get_codelists <- function(dataflow_id, max_tries, base_url, api_key) {
  dataflows <- get_dataflows(max_tries, base_url, api_key)
  dataflow <- dataflows[dataflows$DataflowID == dataflow_id, ]
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
    httr2::req_url_query(references = "codelist") |>
    httr2::req_retry(max_tries)

  resp <- req |> httr2::req_perform()

  resp_list <- resp |> httr2::resp_body_xml() |> xml2::as_list()

  purrr::map(
    resp_list$Structure$Structures$Codelists,
    \(codelist) extract_codes(codelist, dataflow_id)
  ) |>
    purrr::list_rbind()
}

extract_codes <- function(codelist, dataflow_id) {
  id <- attr(codelist, "id")
  codelist <- codelist[-1]

  purrr::map(codelist, \(code) extract_code(code, id)) |> purrr::list_rbind()
}

extract_code <- function(code, id) {
  tibble::tibble(
    CodelistID = id,
    CodeID = attr(code, "id"),
    Name = c(code$Name[[1]]),
  )
}

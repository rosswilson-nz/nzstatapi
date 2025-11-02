nzstat_get_codelists <- function(
  dataflow_id,
  dimension_ids,
  max_tries = 10L,
  base_url = get_base_url(),
  api_key = get_api_key()
) {
  codelists <- get_codelists(dataflow_id, max_tries, base_url, api_key)

  if (missing(dimension_ids)) {
    codelists
  } else {
    codelists[codelists$dimension_id %in% dimension_ids, ]
  }
}

get_codelists <- function(dataflow_id, max_tries, base_url, api_key) {
  dataflows <- nzstat_get_dataflows()
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
  name <- c(codelist[[1]])
  id <- gsub(paste0("(^CL_)|(_", dataflow_id, "$)"), "", attr(codelist, "id"))
  codelist <- codelist[-1]

  purrr::map(codelist, \(code) extract_code(code, id)) |> purrr::list_rbind()
}

extract_code <- function(code, id) {
  tibble::tibble(
    dimension_id = id,
    code_id = attr(code, "id"),
    name = c(code$Name[[1]]),
  )
}

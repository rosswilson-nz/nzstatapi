nzstat_get_codelists <- function(dataflow_id, dimension_ids) {
  base_url <- getOption(
    "NZSTAT_BASE_URL",
    "https://api.data.stats.govt.nz/rest/"
  )
  key <- Sys.getenv("NZSTAT_API_KEY")

  codelists <- get_codelists(dataflow_id)

  codelists[codelists$dimension_id %in% dimension_ids, ]
}

get_codelists <- function(dataflow_id) {
  base_url <- getOption(
    "NZSTAT_BASE_URL",
    "https://api.data.stats.govt.nz/rest/"
  )
  key <- Sys.getenv("NZSTAT_API_KEY")

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
    httr2::req_headers(
      "Ocp-Apim-Subscription-Key" = key,
      "Accept" = "application/xml",
      "user-agent" = "nzstatapi/0.0.0.9000 (Language=R/4.51)",
      "Cache-Control" = "no-cache"
    ) |>
    httr2::req_url_path_append(ref) |>
    httr2::req_url_query(references = "codelist") |>
    httr2::req_retry(max_tries)

  resp <- req |> httr2::req_perform()

  resp_list <- resp |> httr2::resp_body_xml() |> xml2::as_list()

  purrr::map(
    resp_list$Structure$Structures$Codelists,
    \(codelist) extract_codes(codelist)
  ) |>
    purrr::list_rbind()
}

extract_codes <- function(codelist) {
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

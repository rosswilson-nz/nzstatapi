#' Get dataflow definitions for all available Aotearoa Data Explorer datasets
#'
#' Retrieves, formats, and optionally filters all available dataflow definitions from

nzstat_get_dataflows <- function(max_tries = 10L, search = NULL) {
  base_url <- getOption(
    "NZSTAT_BASE_URL",
    "https://api.data.stats.govt.nz/rest/"
  )
  key <- Sys.getenv("NZSTAT_API_KEY")

  req <- httr2::request(base_url) |>
    httr2::req_headers(
      "Ocp-Apim-Subscription-Key" = key,
      "Accept" = "application/json",
      "user-agent" = "nzstatapi/0.0.0.9000 (Language=R/4.51)"
    ) |>
    httr2::req_url_path_append("dataflow/STATSNZ/all") |>
    httr2::req_url_query(detail = "full") |>
    httr2::req_retry(max_tries) |>
    httr2::req_cache(tempdir())

  resp <- req |> httr2::req_perform()

  resp_list <- resp |> httr2::resp_body_json()

  tbl <- purrr::map(
    resp_list$references,
    extract_dataflow_catalogue
  ) |>
    purrr::list_rbind()

  if (!is.null(search)) {
    tbl <- tbl[grepl(search, tbl$Name, ignore.case = TRUE), ]
  }

  tbl
}

extract_dataflow_catalogue <- function(dataflow) {
  tibble::tibble(
    Name = dataflow$name,
    DataflowID = dataflow$id,
    AgencyID = dataflow$agencyID,
    Version = dataflow$version
  )
}

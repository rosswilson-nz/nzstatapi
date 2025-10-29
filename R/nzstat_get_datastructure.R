nzstat_get_datastructure <- function(dataflow_id, max_tries = 10L) {
  base_url <- getOption(
    "NZSTAT_BASE_URL",
    "https://api.data.stats.govt.nz/rest/"
  )
  api_key <- Sys.getenv("NZSTAT_API_KEY")

  dataflows <- nzstat_get_dataflows()
  dataflow <- dataflows[dataflows$DataflowID == dataflow_id, ]
  ref <- paste(
    "dataflow",
    dataflow$AgencyID,
    dataflow$DataflowID,
    dataflow$Version,
    "all",
    sep = "/"
  )

  req <- httr2::request(base_url) |>
    httr2::req_headers(
      "Ocp-Apim-Subscription-Key" = api_key,
      "Accept" = "application/xml",
      "user-agent" = "nzstatapi/0.0.0.9000 (Language=R/4.51)",
      "Cache-Control" = "no-cache"
    ) |>
    httr2::req_url_path_append(ref) |>
    httr2::req_url_query(references = "datastructure") |>
    httr2::req_retry(max_tries)

  resp <- req |> httr2::req_perform()

  resp_list <- resp |> httr2::resp_body_xml() |> xml2::as_list()

  dsd <- purrr::map(
    resp_list$Structure$Structures$DataStructures$DataStructure$DataStructureComponents$DimensionList,
    \(dimension) extract_datastructure_dimension(dimension, dataflow_id)
  ) |>
    purrr::list_rbind()

  dsd[order(dsd$position), ]
}

extract_datastructure_dimension <- function(dimension, dataflow_id) {
  tibble::tibble(
    dimension_id = sub(paste0("_", dataflow_id), "", attr(dimension, "id")),
    position = as.integer(attr(dimension, "position"))
  )
}

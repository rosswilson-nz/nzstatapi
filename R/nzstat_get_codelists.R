#' Get codelists for dimensions of an Aotearoa Data Explorer dataflow
#'
#' Retrieves and formats codelists for one or more dimensions of an Aotearoa
#'     Data Explorer dataflow.
#'
#' @param dataflow_id String. The DataflowID of a dataflow in the API.
#' @param dimensions Character vector. Dimensions for which to retrieve
#'     codelists. Should correspond to elements in the `Name` column of
#'     `nzstat_get_datastructure(dataflow_id)`.
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
nzstat_get_codelists <- function(
  dataflow_id,
  dimensions,
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
  if (!(missing(dimensions) || rlang::is_bare_character(dimensions))) {
    cli::cli_abort(c(
      "{.var dimensions} must be a character vector",
      x = "You've supplied {.type {dimensions}}"
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
  if (is.null(the$dataflows)) {
    the$dataflows <- get_dataflows(max_tries, base_url, api_key)
  }
  if (!(dataflow_id %in% the$dataflows$DataflowID)) {
    cli::cli_abort(c(
      "{.var dataflow_id}={.val {dataflow_id}} not found",
      i = "Please check {.fn nzstat_get_dataflows} to select an available dataflow"
    ))
  }
  dataflow <- as.list(the$dataflows[the$dataflows$DataflowID == dataflow_id, ])

  if (is.null(the$datastructures[[dataflow_id]])) {
    the$datastructures[[dataflow_id]] <- get_datastructures(
      dataflow,
      max_tries,
      base_url,
      api_key
    )
  }
  if (
    !(missing(dimensions) ||
      all(dimensions %in% the$datastructures[[dataflow_id]]$Name))
  ) {
    wrong_ids <- dimensions[
      !(dimensions %in% the$datastructures[[dataflow_id]]$Name)
    ]
    cli::cli_abort(c(
      "Unknown {.var dimensions} {.val {wrong_ids}}",
      i = "Please check {.fn nzstat_get_datastructures} to confirm available dimensions"
    ))
  }

  # Perform request ----
  if (is.null(the$codelists[[dataflow_id]])) {
    the$codelists[[dataflow_id]] <- get_codelists(
      dataflow,
      max_tries,
      base_url,
      api_key
    )
  }

  if (missing(dimensions)) {
    dimensions <- the$datastructures[[dataflow_id]]$Name
  }
  codelist_ids <- the$datastructures[[dataflow_id]][
    the$datastructures[[dataflow_id]]$Name %in% dimensions,
  ]$CodelistID

  purrr::map2(dimensions, codelist_ids, \(dim, cl) {
    out <- the$codelists[[dataflow_id]][
      the$codelists[[dataflow_id]]$CodelistID %in% cl,
    ]
    out$Dimension <- dim
    out[, c("Dimension", "CodeID", "Name")]
  }) |>
    purrr::list_rbind()
}

get_codelists <- function(dataflow, max_tries, base_url, api_key) {
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
    httr2::req_url_query(references = "codelist")

  if (max_tries > 1) {
    req <- req |> httr2::req_retry(max_tries)
  }

  resp <- req |>
    httr2::req_perform() |>
    httr2::resp_body_xml() |>
    xml2::as_list()

  purrr::map(
    resp$Structure$Structures$Codelists,
    \(codelist) extract_codes(codelist)
  ) |>
    purrr::list_rbind()
}

extract_codes <- function(codelist) {
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

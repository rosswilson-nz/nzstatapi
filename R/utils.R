get_base_url <- function() {
  getOption(
    "NZSTAT_BASE_URL",
    "https://api.data.stats.govt.nz/rest/"
  )
}

#' Store Aotearoa Data Explorer API key in environment variable
#'
#' A helper function to set the `NZSTAT_API_KEY` env var.
#'
#' @param key String: API key. If not specified, [askpass::askpass()] will
#'     be used to provide a prompt to enter the key.
#'
#' @export
set_api_key <- function(key = NULL) {
  if (is.null(key)) {
    key <- askpass::askpass("Please enter your API subscription key")
  }
  Sys.setenv("NZSTAT_API_KEY" = key)
  cli::cli_inform(c(
    v = "Key saved to env var {.var NZSTAT_API_KEY}",
    i = "To store this permanently, add the key to your {.file .Renviron} file"
  ))
}

get_api_key <- function() {
  key <- Sys.getenv("NZSTAT_API_KEY")
  if (identical(key, "")) {
    cli::cli_abort(c(
      "Accessing the Aotearoa Data Explorer API service requires a subscription key",
      x = "No API key found.",
      i = "Register at {.url https://portal.apis.stats.govt.nz}",
      i = "Provide the key in the {.field NZSTAT_API_KEY} env var or the {.arg api_key} argument."
    ))
  }
  key
}

make_user_agent <- function() {
  glue::glue(
    "nzstatapi/{packageVersion(\"nzstatapi\")} (Language=R/{package_version(R.version)})"
  )
}

refresh_metadata <- function() {
  the$dataflows <- NULL
  the$datastructures <- NULL
  the$codelists <- NULL
}

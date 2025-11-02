get_base_url <- function() {
  getOption(
    "NZSTAT_BASE_URL",
    "https://api.data.stats.govt.nz/rest/"
  )
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

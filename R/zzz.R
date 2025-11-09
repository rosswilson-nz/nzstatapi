.onLoad <- function(libname, pkgname) {
  # cli::cli_inform("Loading {.pkg nzstatapi} package.")
  api_key <- tryCatch(get_api_key(), error = \(e) "")
  if (api_key == "") {
    cli::cli_inform(c(
      i = "Accessing the Aotearoa Data Explorer API service requires a subscription key.",
      i = "Register at {.url https://portal.apis.stats.govt.nz}.",
      i = "Provide the key in the {.field NZSTAT_API_KEY} env var or the {.arg api_key} argument."
    ))
  } else {
    cli::cli_alert_info("{.pkg nzstatapi}: Loading available tables.")
    the$dataflows <- tryCatch(
      get_dataflows(
        max_tries = 10L,
        base_url = get_base_url(),
        api_key = api_key
      ),
      error = \(e) cli::cli_alert_danger("Unable to access tables.")
    )
  }
}

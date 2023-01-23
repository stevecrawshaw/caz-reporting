
path <- "data/Annex 1 Air Quality Monitoring Reporting Template (1).xlsx"
dt_template <- read_excel(path = path,
                          sheet = "i) Monitoring Checklist", range = "B6:AT7")

labels <- names(dt_template)

names <- make_clean_names(labels)

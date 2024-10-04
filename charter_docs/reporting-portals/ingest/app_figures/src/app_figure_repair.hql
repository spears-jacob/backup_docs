msck repair table ${env:TMP_db}.app_figures_all_sales;
analyze table ${env:TMP_db}.app_figures_all_sales compute statistics;

msck repair table ${env:TMP_db}.app_figures_daily_sales;
analyze table ${env:TMP_db}.app_figures_daily_sales compute statistics;

msck repair table ${env:TMP_db}.app_figures_us_sales;
analyze table ${env:TMP_db}.app_figures_us_sales compute statistics;

msck repair table ${env:TMP_db}.app_figures_us_daily_sales;
analyze table ${env:TMP_db}.app_figures_us_daily_sales compute statistics;

msck repair table ${env:TMP_db}.app_figures_app_details;
analyze table ${env:TMP_db}.app_figures_app_details compute statistics;

msck repair table ${env:TMP_db}.app_figures_ratings;
analyze table ${env:TMP_db}.app_figures_ratings compute statistics;

msck repair table ${env:TMP_db}.app_figures_daily_ranks;
analyze table ${env:TMP_db}.app_figures_daily_ranks compute statistics;

msck repair table ${env:TMP_db}.app_figures_reviews;
analyze table ${env:TMP_db}.app_figures_reviews compute statistics;

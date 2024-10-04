#### Context
* Vidgrid and Video Guide are interchangeable terms.
* Video Guide is the setting on Spectrum Guide where a small video feed of that channel exists to the left of channel when viewing the guide.
* Standard Guide lacks this video feed.
* When a customer goes to view a guide, they will either see a Standard Guide or a Video Guide.
* Users can change this setting in multiple ways, including a single button push on the remote control.

#### Venona Data:
1. Determine when a customer SET a certain guide type using prior to Spectrum Guide App 17.2 Release:
    ```SQL
    AND visit__application_details__application_name = 'Spectrum Guide'
    AND state__view__current_page__elements__standardized_name IN ('showStandardGuide', 'showVideoGuide')
    AND message__name IN ('selectAction')
    ```

2. Determine when a customer has a Guide Screen pageView:
    ```SQL
    AND visit__application_details__application_name = 'Spectrum Guide'
    AND message__name IN ('pageView')
    AND state__view__current_page__page_name IN ('guide')
    ```
3. Determine guide page views per guide type post the Spectrum Guide App 17.2 release
    ```SQL
    AND visit__application_details__application_name = 'Spectrum Guide'
    AND message__name IN ('pageView')
    AND state__view__current_page__page_name IN ('guide')
    AND state__view__current_page__page_display_type IN ('videoGuide' , 'standardGuide')
    ```

#### Data Disclaimers
* Prior to 2017-10-25, Venona did not tag the Guide Type associated with a Guide Page View.
* On 2017-10-25, a new Spectrum Guide App release 17.2 was introduced to certain markets:
  1. Eastern NC,
  2. Greenville, SC / Georgia,
  3. Kansas City / Lincoln,
  4. Pacific Northwest
* Post 2017-10-25, Guide Type Associated with Guide Page Views will be tagged if the market receive the new 17.2 release

#### Key Metrics

#### Venona
1. Percentage of HHs and STBs Video Guide, Standard Guide, or both settings.
  * If an account is linked to 'showStandardGuide' then Standard Guide
  * If an account is linked to 'showVideoGuide' then Video Guide
  * Else if account linked to showStandardGuide' AND 'showVideoGuide' then both
  * Account must be linked to a 'showStandardGuide' or 'showVideoGuide' instance that is associated with a 'selectAction'. This means that a certain guide type was set.
2. Guide Type associated with Guide Page Views
  * We examine what guide type was triggered when a guide page view instance is triggered.




















# **Archived**

#### Adobe Data
To determine if it is vidgrid or not, use the field and mapping state__view__current_page__settings['VidGrid State']
  ```SQL
    if state__view__current_page__settings['VidGrid State'] = 'In View' then state is vid grid, else if
    state__view__current_page__settings['VidGrid State'] = 'Not In View'  then state is standard guide.
    This is only populated on a 'pageview' of Guide
    ```


#### Key Metrics

#### Adobe
1. PERCENTAGE OF GUIDE PAGEVIEWS THAT ARE VIDGRID.
  * A pageview of guide will either have state__view__current_page__settings['VidGrid State'] = 'Not In View' for Standard Guide
  * OR state__view__current_page__settings['VidGrid State'] = 'In View' for Video Grid.  
  * These are used to calculate % of page views.


2. Percentage of HHs and STBs with at least one Vid Grid Page View.
    * if a stb or hh had at least one page view where state__view__current_page__settings['VidGrid State'] = 'In View' then they are counted
    as having at least one page view.  The denominator is HHs/STBs with at least one Guide Screen Page view.
    * This number will always be equal to or higher than % of households that had at least one day of vid grid on, per the next metric because if a user switches from vid grid to standard grid and back in the same day, that day would not be counted as having vid grid on.
    * It is suspected (but not proven) that people may switch the vid grid setting on by accident due to the remote control, then revert the setting
    on the same day.

3. DAYS A STB HAD VIDGRID ON
    * A stb is considered to have vidgrid on for that day if they have at least one guide page view with vidgrid on, and no pageview with standard guide on.  In order to be counted as zero days, a STB must have some activity on Spectrum Guide, but no full day where each guide screen pageview was Vid Grid.

#### Data Disclaimers:
    * Information concerning Vid Grid usage for pre December 1, was generated on the CTEC cluster due to adobe events not existing for January 5, 2016 - November 30, 2016 on GV.  
    * The remainder was calculated on GV.  Only change between the queries was the field `date` renamed to partition_date_time.

-- materialized: (duid,delta,overwrite)

create or replace temp table duid_aemo as
SELECT
    DUID as DUID,
    first(Region) as Region,
    first("Fuel Source - Descriptor") as FuelSourceDescriptor,
    first(Participant) as Participant
from read_xlsx('https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/NEM-Registration-and-Exemption-List.xls',
    sheet='PU and Scheduled Loads',
    ALL_VARCHAR=true
)
where length(DUID) > 2
group by DUID;

create or replace temp table states(RegionID varchar, States varchar);

insert into states values
    ('WA1', 'Western Australia'),
    ('QLD1', 'Queensland'),
    ('NSW1', 'New South Walles'),
    ('TAS1', 'Tasmania'),
    ('SA1', 'South Australia'),
    ('VIC1', 'Victoria');

create or replace temp table duid_final as
with x as (
    select 'WA1' as Region, "Facility Code" as DUID, "Participant Name" as Participant
    from read_csv_auto('https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv')
),
tt as (
    select *
    from read_csv_auto('https://github.com/djouallah/aemo_fabric/raw/main/WA_ENERGY.csv', header=1)
),
duid_wa as (
    select x.DUID, x.Region, Technology as FuelSourceDescriptor, x.Participant
    from x
    left join tt on x.DUID = tt.DUID
),
duid_all as (
    select * from duid_aemo
    union all
    select * from duid_wa
)
select
    DUID,
    Region,
    FuelSourceDescriptor,
    Participant,
    states.States as State
from duid_all a
join states on a.Region = states.RegionID;
select * from duid_final
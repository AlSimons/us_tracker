-- Fixup Chicago in the admin1 column.
update datum set location_jhu_key = 'Chicago, Illinois, US' where  location_jhu_key = 'Chicago, US';
delete from location where admin1='Chicago';
--


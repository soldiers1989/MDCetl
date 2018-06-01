from enum import Enum


class SQL(Enum):

    bap_categorical = '''SELECT S.Name, F.FiscalYear, F.FiscalQuarter,
                        SUM(F.AnnualRevenue) REVENUE,
                        SUM(NumberEmployees)  Employees,
                        Sum(AdvisoryServicesHours) AdvisoryHours,
                        SUM(VolunteerMentorHours) VolunteerHours,
                        SUM(FundingToDate) FundingTODate
                        FROM MDCReport.BAPQ.FactRICCompany F
                        INNER JOIN {} S ON S.ID = F.{}
                        WHERE F.FiscalYear IS NOT NULL
                        GROUP BY S.Name, F.FiscalYear, F.FiscalQuarter
                        ORDER BY F.FiscalYear
                        '''

    bap_types = '''SELECT S.Name, F.{},  F.FiscalYear,
                  SUM(F.AnnualRevenue) REVENUE,
                  SUM(NumberEmployees)  Employees,
                  Sum(AdvisoryServicesHours) AdvisoryHours,
                  SUM(VolunteerMentorHours) VolunteerHours,
                  SUM(FundingToDate) FundingTODate
                  FROM MDCReport.BAPQ.FactRICCompany F
                  INNER JOIN {} S ON S.ID = F.{}
                  WHERE F.FiscalYear IS NOT NULL
                  GROUP BY S.Name, F.{}, F.FiscalYear
                  ORDER BY F.FiscalYear
                      '''


class Category(Enum):
    DataSource = 1
    Industry = 2
    Stage = 3
    Potential = 4
    Social = 5
    Youth = 6


class Keys(Enum):
    mapbox_access_token = 'pk.eyJ1IjoibXVzc2llbiIsImEiOiJjamhxYzhtdHUxY2M1MzZxYXlkOXYxNGFuIn0.Sc9hAxJUAXBkFzKACNGvyQ'

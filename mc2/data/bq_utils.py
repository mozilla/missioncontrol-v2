class BqLocation:
    def __init__(
        self,
        table,
        dataset="analysis",
        project_id="moz-fx-data-derived-datasets",
        base_project_id="moz-fx-data-derived-datasets",
    ):
        self.table = table
        self.dataset = dataset
        self.project_id = project_id
        self.base_project_id = base_project_id

    @property
    def sql(self):
        return f"`{self.project_id}`.{self.dataset}.{self.table}"
        # return "`{}`.{}.{}".format(self.project_id, self.dataset, self.table)

    @property
    def cli(self):
        return f"{self.project_id}:{self.dataset}.{self.table}"

    @property
    def no_proj(self):
        return f"{self.dataset}.{self.table}"

    @property
    def sql_dataset(self):
        return f"`{self.project_id}`.{self.dataset}"


def check_table_exists(query_func, bq_loc: BqLocation):
    q = f"""
    SELECT count(*) as exist FROM {bq_loc.sql_dataset}.__TABLES__
    WHERE table_id='{bq_loc.table}'
    """
    [row] = query_func(q)
    [exists] = row.values()
    return bool(exists)

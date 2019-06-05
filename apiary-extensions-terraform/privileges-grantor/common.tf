/**
 * Copyright (C) 2019 Expedia Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

locals {
  instance_alias = "${ var.instance_name == "" ? "privilege-grantor" : format("privilege-grantor-%s",var.instance_name) }"
}

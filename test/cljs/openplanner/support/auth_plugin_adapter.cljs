(ns openplanner.support.auth-plugin-adapter
  "Test-only nbb loadFile ABI for the unchanged reference HTTP fixture."
  (:require [openplanner.extern.js.fastify.auth :as fastify]
            [openplanner.infra.http.auth :as auth]))

#js {:authPlugin (fastify/auth-plugin auth/decide-auth)}

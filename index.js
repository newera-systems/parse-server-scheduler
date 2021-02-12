const { CronJob } = require('cron')
const moment      = require('moment')
const Parse       = require('parse/node')
const rp          = require('request-promise')


const PARSE_TIMEZONE = 'UTC'


class ParseServerScheduler {
    cronJobs = {}

    constructor() {

    }

    async start() {

        if (!Parse.applicationId) {
            throw new Error('Parse is not initialized')
        }

        try {
            await this._recreateScheduleForAllJobs()
        } catch (error) {
            throw error
        }


        // Recreates schedule when a job schedule has changed
        Parse.Cloud.afterSave('_JobSchedule', async (request) => {
            recreateSchedule(request.object)
        })

        // Destroy schedule for removed job
        Parse.Cloud.afterDelete('_JobSchedule', async (request) => {
            destroySchedule(request.object)
        })
    }

    async stop()
    {
        await this._destroySchedules()
    }

    /**
     * Parse job schedule object
     * @typedef {Object} _JobSchedule
     * @property {String} id The job id
     */

    async _retreiveJobBy(jobId) {

        const jobObject = await Parse.Object.extend('_JobSchedule')
          .createWithoutData(job)
          .fetch({
              useMasterKey : true
          })
        if (!jobObject) {
            throw new Error(`No _JobSchedule was found with id ${job}`)
        }

        return jobObject
    }

    /**
     * Recreate the cron schedules for a specific _JobSchedule or all _JobSchedule objects
     * @param {_JobSchedule | string} [job=null] The job schedule to recreate. If not specified, all jobs schedules will be recreated.
     * Can be a _JobSchedule object or the id of a _JobSchedule object.
     */
    async _recreateSchedule(job) {
        if (job instanceof String || typeof job === 'string') {
            //throw new Error('Invalid job type. Must be a string or a _JobSchedule')
            job = this._retreiveJobBy(job)
        }

        if (!job instanceof Parse.Object || job.className !== '_JobSchedule') {
            throw new Error('Invalid job type. Must be a _JobSchedule')

        }

        this._recreateJobSchedule(job)
    }

    /**
     * (Re)creates all schedules (crons) for all _JobSchedule from the Parse server
     */

    async _recreateScheduleForAllJobs() {

        try {
            const results = await new Parse.Query('_JobSchedule').find({
                useMasterKey : true
            })

            await this._destroySchedules()

            for (let job of results) {
                try {
                    await this._recreateJobSchedule(job)
                } catch (error) {
                    console.error(error)
                }
            }

            console.log(`${Object.keys(cronJobs).length} job(s) scheduled.`)


        } catch (error) {
            throw error
        }
    }

    /**
     * (Re)creates the schedule (crons) of a _JobSchedule
     * @param {_JobSchedule} job The _JobSchedule
     */
    _recreateJobSchedule(job) {

        this._destroySchedule(job.id)
        this.cronJobs[job.id] = this._createCronJobs(job)
    }

    /**
     * Stop all jobs and remove them from the list of jobs
     */
    _destroySchedules() {
        for (let key of Object.keys(this.cronJobs)) {
            this._destroySchedule(key)
        }

        this.cronJobs = {}
    }

    /**
     * Destroy a planned cron job
     * @param {String} id The _JobSchedule id
     */
    _destroySchedule(id) {
        const jobs = this.cronJobs[id]

        if (!jobs) {
            return
        }

        for (let job of jobs) {
            job.stop()
        }

        delete this.cronJobs[id]
    }

    _buildCronString(job) {
        // Periodic job. Create a cron to launch the periodic job a the start date.
        let timeOfDay       = moment(job.get('timeOfDay'), 'HH:mm:ss.Z')
          .utc()
        const daysOfWeek    = job.get('daysOfWeek')
        const repeatMinutes = job.get('repeatMinutes')
        const cronDoW       = (daysOfWeek) ? this._daysOfWeekToCronString(daysOfWeek) : '*'
        const minutes       = repeatMinutes % 60
        const hours         = Math.floor(repeatMinutes / 60)

        let cron = '0 '
        // Minutes
        if (minutes) {
            cron += `${timeOfDay.minutes()}-59/${minutes} `
        } else {
            cron += `0 `
        }

        // Hours
        cron += `${timeOfDay.hours()}-23`
        if (hours) {
            cron += `/${hours}`
        }
        cron += ' '

        // Day of month
        cron += '* '

        // Month
        cron += '* '

        // Days of week
        cron += cronDoW

        return cron
    }


    _createOneTimeCronJob(job, startTime) {

        return new CronJob(
          startTime,
          () => { // On tick
              this._performJob(job)
          },
          null, // On complete
          false, // true, // Start
          PARSE_TIMEZONE // Timezone
        )
    }

    _createCronJobs(job) {
        const startAfter    = new Date(job.get('startAfter'))
        const repeatMinutes = job.get('repeatMinutes')
        const jobName       = job.get('jobName')
        const params        = job.get('params')
        const now           = moment()

        // Launch just once
        if (!repeatMinutes) {
            return [
                this._createOneTimeCronJob(job, startAfter)
            ]
        }

        const actualJob = this._createOneTimeCronJob(job, this._buildCronString(job))

        if (!startAfter) {
            return [actualJob]
        }

        // Otherwise, schedule a cron that is going to launch our actual cron at the time of the day
        const startCron = new CronJob(
          startAfter,
          () => { // On tick
              console.log('Start the cron')
              actualJob.start()
          },
          null, // On complete
          true, // Start
          PARSE_TIMEZONE // Timezone
        )

        return [startCron, actualJob]
    }

    /**
     * Converts the Parse scheduler days of week
     * @param {Array} daysOfWeek An array of seven elements for the days of the week. 1 to schedule the task for the day, otherwise 0.
     */
    _daysOfWeekToCronString(daysOfWeek) {
        const daysNumbers = []
        for (let i = 0; i < daysOfWeek.length; i++) {
            if (daysOfWeek[i]) {
                daysNumbers.push((i + 1) % 7)
            }
        }
        return daysNumbers.join(',')
    }

    /**
     * Perform a background job
     * @param {String} jobName The job name on Parse Server
     * @param {Object=} params The parameters to pass to the request
     */
    async _performJob(job, params) {
        try {
            const jobName = job.get('jobName')

            const request = rp({
                method  : 'POST',
                uri     : Parse.serverURL + '/jobs/' + jobName,
                headers : {
                    'X-Parse-Application-Id' : Parse.applicationId,
                    'X-Parse-Master-Key'     : Parse.masterKey
                },
                json    : true // Automatically parses the JSON string in the response
            })
            if (params) {
                request.body = params
            }
            console.error(`Job ${jobName} launched.`)
        } catch (error) {
            console.error(error)
        }


    }
}


module.exports = {
    ParseServerScheduler,
}

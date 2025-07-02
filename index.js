const { Client, GatewayIntentBits, SlashCommandBuilder, EmbedBuilder, AttachmentBuilder, PermissionFlagsBits, ChannelType, ButtonBuilder, ButtonStyle, ActionRowBuilder } = require('discord.js');
const { ApifyClient } = require('apify-client');
const ExcelJS = require('exceljs');
const express = require('express');

// Initialize Express app for Koyeb health checks
const app = express();
const PORT = process.env.PORT || 8000;

// Cache management system
const botCache = {
    channelCounts: new Map(),
    progressSettings: null,
    lastUpdate: null,
    urlCache: new Map(),
    leaderboardMessages: new Map(),
    verifiedAccounts: new Map(), // Cache verified accounts
    userChannels: new Map(), // Cache user ticket channels
    viewUpdateQueue: [], // Queue for automated view updates
    lastViewUpdate: null,
    clear: function() {
        this.channelCounts.clear();
        this.progressSettings = null;
        this.lastUpdate = null;
        this.urlCache.clear();
        this.leaderboardMessages.clear();
        this.verifiedAccounts.clear();
        this.userChannels.clear();
        this.viewUpdateQueue = [];
        this.lastViewUpdate = null;
        console.log('🗑️ Bot cache cleared completely');
    },
    getMemoryUsage: function() {
        return {
            channelCounts: this.channelCounts.size,
            urlCache: this.urlCache.size,
            leaderboardMessages: this.leaderboardMessages.size,
            verifiedAccounts: this.verifiedAccounts.size,
            userChannels: this.userChannels.size,
            viewUpdateQueue: this.viewUpdateQueue.length,
            progressSettings: this.progressSettings ? 'Set' : 'Not set'
        };
    }
};

// Initialize Discord client
let client;

// Health check endpoints for Koyeb
app.get('/', (req, res) => {
    res.status(200).json({
        status: 'Bot is running',
        uptime: process.uptime(),
        memory: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB',
        timestamp: new Date().toISOString(),
        bot_ready: client ? client.isReady() : false,
        cache_info: botCache.getMemoryUsage()
    });
});

app.get('/health', (req, res) => {
    const isReady = client ? client.isReady() : false;
    const memUsage = process.memoryUsage();
    res.status(isReady ? 200 : 503).json({
        status: isReady ? 'healthy' : 'unhealthy',
        bot_ready: isReady,
        ws_ping: client ? client.ws.ping : -1,
        uptime: process.uptime(),
        guilds: client ? client.guilds.cache.size : 0,
        memory_usage: {
            rss: Math.round(memUsage.rss / 1024 / 1024) + 'MB',
            heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024) + 'MB',
            heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024) + 'MB'
        },
        cache_info: botCache.getMemoryUsage()
    });
});

// Force garbage collection endpoint
app.post('/gc', (req, res) => {
    try {
        if (global.gc) {
            const before = process.memoryUsage().heapUsed / 1024 / 1024;
            global.gc();
            const after = process.memoryUsage().heapUsed / 1024 / 1024;
            res.json({
                status: 'success',
                memory_before: Math.round(before) + 'MB',
                memory_after: Math.round(after) + 'MB',
                freed: Math.round(before - after) + 'MB'
            });
        } else {
            res.json({ status: 'error', message: 'Garbage collection not available' });
        }
    } catch (error) {
        res.status(500).json({ status: 'error', message: error.message });
    }
});

client = new Client({
    intents: [
        GatewayIntentBits.Guilds,
        GatewayIntentBits.GuildMessages,
        GatewayIntentBits.MessageContent,
        GatewayIntentBits.GuildVoiceStates,
    ],
});

// Initialize Apify clients with different tokens
const apifyViewsClient = new ApifyClient({
    token: process.env.APIFY_VIEWS_TOKEN,
});

const apifyVerifyClient = new ApifyClient({
    token: process.env.APIFY_VERIFY_TOKEN,
});

// Configuration
const CONFIG = {
    // Views counting configuration
    APIFY_VIEWS_TASK_ID: process.env.APIFY_VIEWS_TASK_ID || 'yACwwaUugD0F22xUU',
    
    // Account verification configuration
    APIFY_VERIFY_TASK_ID: process.env.APIFY_VERIFY_TASK_ID || 'bio_scraper_task_id',
    
    // Channel names
    VIEWS_CHANNEL_NAME: process.env.VIEWS_CHANNEL_NAME || 'views',
    LEADERBOARD_CHANNEL_NAME: process.env.LEADERBOARD_CHANNEL_NAME || 'leaderboard',
    LOGS_CHANNEL_NAME: process.env.LOGS_CHANNEL_NAME || 'logs',
    EXECUTION_CHANNEL_NAME: process.env.EXECUTION_CHANNEL_NAME || 'view-counting-execution',
    
    // Progress settings
    PROGRESS_VOICE_CHANNEL_PREFIX: '📊 Progress: ',
    
    // Cache and timing
    CACHE_DURATION: 5 * 60 * 1000, // 5 minutes
    INTERACTION_TIMEOUT: 14 * 60 * 1000, // 14 minutes
    MAX_MESSAGES_PER_BATCH: 500,
    LEADERBOARD_PER_PAGE: 10,
    
    // View update automation
    VIEW_UPDATE_INTERVAL: 60 * 60 * 1000, // 1 hour base interval
    REPORT_GENERATION_DELAY: 6 * 60 * 60 * 1000, // 6 hours
    MIN_VIEWS_THRESHOLD: 500, // Minimum views to count in totals
    
    // Verification settings
    VERIFICATION_CODE_LENGTH: 4,
    TICKET_CATEGORY_NAME: 'User Tickets',
};

// Role-based permission configuration
const ROLE_CONFIG = {
    CAMPAIGN_ROLES: ['Campaign Manager', 'Social Media Manager'],
    ADMIN_ROLES: ['Administrator'],
    ADMIN_ONLY: ['status', 'clearcache', 'stats'],
    CAMPAIGN_COMMANDS: ['viewscount', 'progressbar', 'updateprogress'],
    USER_COMMANDS: ['verify', 'submit', 'list'],
};

// Permission checker function
const checkPermissions = (interaction, commandName) => {
    const member = interaction.member;
    
    if (interaction.guild.ownerId === member.id) {
        return { allowed: true, reason: 'Server Owner' };
    }
    
    if (member.permissions.has(PermissionFlagsBits.Administrator)) {
        return { allowed: true, reason: 'Administrator' };
    }
    
    if (ROLE_CONFIG.ADMIN_ONLY.includes(commandName)) {
        return { 
            allowed: false, 
            reason: 'This command requires Administrator permissions.' 
        };
    }
    
    if (ROLE_CONFIG.CAMPAIGN_COMMANDS.includes(commandName)) {
        const hasRole = ROLE_CONFIG.CAMPAIGN_ROLES.some(roleName => {
            return member.roles.cache.some(role => role.name === roleName);
        });
        
        if (hasRole) {
            const userRole = ROLE_CONFIG.CAMPAIGN_ROLES.find(roleName => {
                return member.roles.cache.some(role => role.name === roleName);
            });
            return { allowed: true, reason: `Role: ${userRole}` };
        }
        
        return { 
            allowed: false, 
            reason: `You need one of these roles: ${ROLE_CONFIG.CAMPAIGN_ROLES.join(', ')}` 
        };
    }
    
    // User commands are allowed for everyone
    if (ROLE_CONFIG.USER_COMMANDS.includes(commandName)) {
        return { allowed: true, reason: 'User Command' };
    }
    
    return { allowed: true, reason: 'Default Access' };
};

// Utility functions
const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
};

const generateVerificationCode = () => {
    const letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    const numbers = '0123456789';
    const alphanumeric = letters + numbers;
    
    // First two characters: letters
    let code = '';
    code += letters.charAt(Math.floor(Math.random() * letters.length));
    code += letters.charAt(Math.floor(Math.random() * letters.length));
    
    // Last two characters: random alphanumeric
    code += alphanumeric.charAt(Math.floor(Math.random() * alphanumeric.length));
    code += alphanumeric.charAt(Math.floor(Math.random() * alphanumeric.length));
    
    return code;
};

const logError = async (guild, error, context = '') => {
    try {
        const logsChannel = guild.channels.cache.find(ch => ch.name === CONFIG.LOGS_CHANNEL_NAME);
        if (logsChannel) {
            const errorEmbed = new EmbedBuilder()
                .setTitle('❌ Error Report')
                .setDescription(`**Context:** ${context}\n**Error:** ${error.message}`)
                .addFields(
                    { name: 'Stack Trace', value: `\`\`\`${error.stack?.slice(0, 1000) || 'No stack trace'}\`\`\``, inline: false },
                    { name: 'Timestamp', value: new Date().toISOString(), inline: true }
                )
                .setColor(0xFF0000)
                .setTimestamp();
            
            await logsChannel.send({ embeds: [errorEmbed] });
        }
    } catch (logError) {
        console.error('Failed to log error to channel:', logError);
    }
};

const extractInstagramUrls = (messages) => {
    const urlRegex = /https?:\/\/(?:www\.)?instagram\.com\/(?:reel|p)\/[A-Za-z0-9_-]+/g;
    const urls = new Set();

    messages.forEach(message => {
        const matches = message.content.match(urlRegex);
        if (matches) {
            matches.forEach(url => {
                let cleanUrl = url
                    .replace(/\/+$/, '')
                    .replace(/\?.*$/, '')
                    .replace(/#.*$/, '');

                cleanUrl = cleanUrl.replace(/https?:\/\/(?:www\.)?instagram\.com/, 'https://www.instagram.com');

                if (!cleanUrl.endsWith('/')) {
                    cleanUrl += '/';
                }

                urls.add(cleanUrl);
            });
        }
    });

    return Array.from(urls);
};

const getUserTicketChannel = async (guild, userId) => {
    // Check cache first
    if (botCache.userChannels.has(userId)) {
        const channelId = botCache.userChannels.get(userId);
        const channel = guild.channels.cache.get(channelId);
        if (channel) return channel;
        
        // Channel was deleted, remove from cache
        botCache.userChannels.delete(userId);
    }
    
    // Search for existing channel
    const existingChannel = guild.channels.cache.find(ch => 
        ch.name.startsWith(`ticket-${userId}`) && ch.type === ChannelType.GuildText
    );
    
    if (existingChannel) {
        botCache.userChannels.set(userId, existingChannel.id);
        return existingChannel;
    }
    
    return null;
};

const createUserTicketChannel = async (guild, user) => {
    try {
        // Find or create category
        let category = guild.channels.cache.find(ch => 
            ch.name === CONFIG.TICKET_CATEGORY_NAME && ch.type === ChannelType.GuildCategory
        );
        
        if (!category) {
            category = await guild.channels.create({
                name: CONFIG.TICKET_CATEGORY_NAME,
                type: ChannelType.GuildCategory,
                permissionOverwrites: [
                    {
                        id: guild.roles.everyone,
                        deny: ['ViewChannel'],
                    },
                ],
            });
        }
        
        // Create user ticket channel
        const channel = await guild.channels.create({
            name: `ticket-${user.id}`,
            type: ChannelType.GuildText,
            parent: category.id,
            permissionOverwrites: [
                {
                    id: guild.roles.everyone,
                    deny: ['ViewChannel'],
                },
                {
                    id: user.id,
                    allow: ['ViewChannel', 'SendMessages', 'ReadMessageHistory'],
                },
                // Add Campaign Manager permissions
                ...ROLE_CONFIG.CAMPAIGN_ROLES.map(roleName => {
                    const role = guild.roles.cache.find(r => r.name === roleName);
                    return role ? {
                        id: role.id,
                        allow: ['ViewChannel', 'SendMessages', 'ReadMessageHistory'],
                    } : null;
                }).filter(Boolean),
                // Add Administrator permissions
                {
                    id: guild.roles.cache.find(r => r.permissions.has(PermissionFlagsBits.Administrator))?.id,
                    allow: ['ViewChannel', 'SendMessages', 'ReadMessageHistory'],
                },
            ].filter(Boolean),
        });
        
        // Cache the channel
        botCache.userChannels.set(user.id, channel.id);
        
        return channel;
    } catch (error) {
        await logError(guild, error, 'Creating user ticket channel');
        throw error;
    }
};

const checkDuplicateAccount = async (guild, username) => {
    try {
        // Search through all user ticket channels
        const ticketChannels = guild.channels.cache.filter(ch => 
            ch.name.startsWith('ticket-') && ch.type === ChannelType.GuildText
        );
        
        for (const channel of ticketChannels.values()) {
            try {
                const messages = await channel.messages.fetch({ limit: 100 });
                
                for (const message of messages.values()) {
                    if (message.content.includes(`@${username}`) || message.content.includes(username)) {
                        // Found duplicate
                        const userId = channel.name.replace('ticket-', '');
                        const user = await client.users.fetch(userId).catch(() => null);
                        return {
                            isDuplicate: true,
                            existingUser: user,
                            channel: channel
                        };
                    }
                }
            } catch (error) {
                console.error(`Error checking channel ${channel.name}:`, error);
                continue;
            }
        }
        
        return { isDuplicate: false };
    } catch (error) {
        await logError(guild, error, 'Checking duplicate account');
        return { isDuplicate: false };
    }
};

const scrapeInstagramBio = async (username) => {
    try {
        const input = {
            usernames: [username],
            resultsLimit: 1
        };
        
        const run = await apifyVerifyClient.task(CONFIG.APIFY_VERIFY_TASK_ID).call(input);
        const { items } = await apifyVerifyClient.dataset(run.defaultDatasetId).listItems();
        
        if (items.length > 0) {
            return items[0];
        }
        
        return null;
    } catch (error) {
        console.error('Error scraping Instagram bio:', error);
        throw error;
    }
};

const getVerifiedAccounts = async (channel) => {
    try {
        const messages = await channel.messages.fetch({ limit: 100 });
        const accounts = [];
        
        messages.forEach(message => {
            const lines = message.content.split('\n');
            lines.forEach(line => {
                if (line.includes('@') && line.includes('instagram.com')) {
                    const match = line.match(/@(\w+)/);
                    if (match) {
                        accounts.push(match[1]);
                    }
                }
            });
        });
        
        return accounts;
    } catch (error) {
        console.error('Error getting verified accounts:', error);
        return [];
    }
};

const updateLeaderboard = async (guild, forceUpdate = false) => {
    try {
        const leaderboardChannel = guild.channels.cache.find(ch => ch.name === CONFIG.LEADERBOARD_CHANNEL_NAME);
        if (!leaderboardChannel) {
            console.log(`Leaderboard channel not found: ${CONFIG.LEADERBOARD_CHANNEL_NAME}`);
            return;
        }
        
        // Get all user data from ticket channels
        const ticketChannels = guild.channels.cache.filter(ch => 
            ch.name.startsWith('ticket-') && ch.type === ChannelType.GuildText
        );
        
        const userStats = new Map();
        
        for (const channel of ticketChannels.values()) {
            try {
                const userId = channel.name.replace('ticket-', '');
                const user = await client.users.fetch(userId).catch(() => null);
                if (!user) continue;
                
                const messages = await channel.messages.fetch({ limit: 100 });
                let totalViews = 0;
                let videoCount = 0;
                const accounts = [];
                
                messages.forEach(message => {
                    // Parse account info
                    if (message.content.includes('@') && message.content.includes('instagram.com')) {
                        const lines = message.content.split('\n');
                        lines.forEach(line => {
                            const accountMatch = line.match(/@(\w+)/);
                            if (accountMatch && !accounts.includes(accountMatch[1])) {
                                accounts.push(accountMatch[1]);
                            }
                        });
                    }
                    
                    // Parse view counts from logs
                    const viewMatch = message.content.match(/(\d+) views/);
                    if (viewMatch) {
                        const views = parseInt(viewMatch[1]);
                        if (views >= CONFIG.MIN_VIEWS_THRESHOLD) {
                            totalViews += views;
                        }
                        videoCount++;
                    }
                });
                
                if (totalViews > 0 || accounts.length > 0) {
                    userStats.set(userId, {
                        user: user,
                        totalViews: totalViews,
                        videoCount: videoCount,
                        accounts: accounts
                    });
                }
            } catch (error) {
                console.error(`Error processing channel ${channel.name}:`, error);
                continue;
            }
        }
        
        // Create leaderboard embed
        const sortedUsers = Array.from(userStats.entries())
            .sort((a, b) => b[1].totalViews - a[1].totalViews);
        
        let leaderboardText = '';
        sortedUsers.forEach(([userId, stats], index) => {
            const rank = index + 1;
            const accountsList = stats.accounts.join(', ');
            leaderboardText += `${rank}. ${stats.user.displayName} - ${formatNumber(stats.totalViews)} views (${stats.videoCount} videos)\n`;
            if (accountsList) {
                leaderboardText += `   📱 Accounts: ${accountsList}\n`;
            }
        });
        
        const embed = new EmbedBuilder()
            .setTitle('🏆 User Leaderboard')
            .setDescription(leaderboardText || 'No users with verified accounts found')
            .setColor(0xFFD700)
            .addFields(
                { 
                    name: '📊 Total Stats', 
                    value: `${sortedUsers.length} users tracked`, 
                    inline: false 
                }
            )
            .setTimestamp()
            .setFooter({ text: 'Updated automatically every hour' });
        
        // Send or update leaderboard
        const existingMessageId = botCache.leaderboardMessages.get(guild.id);
        let leaderboardMessage = null;
        
        if (existingMessageId) {
            try {
                leaderboardMessage = await leaderboardChannel.messages.fetch(existingMessageId);
                await leaderboardMessage.edit({ embeds: [embed] });
            } catch (error) {
                botCache.leaderboardMessages.delete(guild.id);
                leaderboardMessage = null;
            }
        }
        
        if (!leaderboardMessage) {
            leaderboardMessage = await leaderboardChannel.send({ embeds: [embed] });
            botCache.leaderboardMessages.set(guild.id, leaderboardMessage.id);
        }
        
        console.log(`✅ Leaderboard updated with ${sortedUsers.length} users`);
        
    } catch (error) {
        console.error('Error updating leaderboard:', error);
        await logError(guild, error, 'Updating leaderboard');
    }
};

const processVideoForViews = async (guild, videoUrl) => {
    try {
        const input = {
            addParentData: false,
            directUrls: [videoUrl],
            enhanceUserSearchWithFacebookPage: false,
            isUserReelFeedURL: true,
            isUserTaggedFeedURL: false,
            resultsLimit: 1,
            resultsType: "posts",
            searchLimit: 1
        };
        
        const run = await apifyViewsClient.task(CONFIG.APIFY_VIEWS_TASK_ID).call(input);
        const { items } = await apifyViewsClient.dataset(run.defaultDatasetId).listItems();
        
        if (items.length > 0 && items[0].videoPlayCount !== undefined) {
            return {
                views: items[0].videoPlayCount,
                likes: items[0].likesCount || 0,
                comments: items[0].commentsCount || 0,
                username: items[0].ownerUsername || 'unknown'
            };
        }
        
        return null;
    } catch (error) {
        console.error('Error processing video for views:', error);
        await logError(guild, error, `Processing video: ${videoUrl}`);
        return null;
    }
};

const automatedViewUpdater = async () => {
    try {
        if (!client.isReady()) return;
        
        for (const guild of client.guilds.cache.values()) {
            try {
                // Check if progress is still active
                const progressSettings = await getProgressSettings(guild);
                if (!progressSettings) continue;
                
                // Get total current views to check if target reached
                const viewsChannel = guild.channels.cache.find(ch => ch.name === CONFIG.VIEWS_CHANNEL_NAME);
                if (!viewsChannel) continue;
                
                const channelCounts = await parseViewsFromChannel(viewsChannel);
                const totalViews = Array.from(channelCounts.values()).reduce((sum, views) => sum + views, 0);
                
                // Stop if target reached
                if (totalViews >= progressSettings.target) {
                    console.log(`🎯 Target reached for ${guild.name}, stopping automated updates`);
                    
                    // Generate final report
                    await generateFinalReport(guild);
                    continue;
                }
                
                // Process one video from the queue
                if (botCache.viewUpdateQueue.length === 0) {
                    // Rebuild queue from all ticket channels
                    await buildViewUpdateQueue(guild);
                }
                
                if (botCache.viewUpdateQueue.length > 0) {
                    const videoData = botCache.viewUpdateQueue.shift();
                    await processQueuedVideo(guild, videoData);
                }
                
            } catch (error) {
                console.error(`Error in automated update for guild ${guild.name}:`, error);
                await logError(guild, error, 'Automated view updater');
            }
        }
        
    } catch (error) {
        console.error('Error in automated view updater:', error);
    }
};

const buildViewUpdateQueue = async (guild) => {
    try {
        const ticketChannels = guild.channels.cache.filter(ch => 
            ch.name.startsWith('ticket-') && ch.type === ChannelType.GuildText
        );
        
        const videos = [];
        
        for (const channel of ticketChannels.values()) {
            try {
                const messages = await channel.messages.fetch({ limit: 50 });
                
                messages.forEach(message => {
                    const urls = extractInstagramUrls([message]);
                    urls.forEach(url => {
                        videos.push({
                            url: url,
                            channelId: channel.id,
                            userId: channel.name.replace('ticket-', '')
                        });
                    });
                });
            } catch (error) {
                console.error(`Error processing channel ${channel.name}:`, error);
                continue;
            }
        }
        
        // Shuffle for random order
        for (let i = videos.length - 1; i > 0; i--) {
            const j = Math.floor(Math.random() * (i + 1));
            [videos[i], videos[j]] = [videos[j], videos[i]];
        }
        
        botCache.viewUpdateQueue = videos;
        
        // Calculate interval based on total videos (24 hours / video count)
        const intervalHours = Math.max(1, Math.floor(24 / videos.length));
        console.log(`📊 Built queue with ${videos.length} videos, ${intervalHours}h intervals`);
        
    } catch (error) {
        console.error('Error building view update queue:', error);
    }
};

const processQueuedVideo = async (guild, videoData) => {
    try {
        const viewData = await processVideoForViews(guild, videoData.url);
        if (!viewData) return;
        
        // Log to views channel
        const viewsChannel = guild.channels.cache.find(ch => ch.name === CONFIG.VIEWS_CHANNEL_NAME);
        if (viewsChannel) {
            const userChannel = guild.channels.cache.get(videoData.channelId);
            const channelName = userChannel ? userChannel.name : 'unknown-channel';
            
            await viewsChannel.send(
                `📊 ${videoData.url} from #${channelName} got ${formatNumber(viewData.views)} views (${viewData.likes} likes, ${viewData.comments} comments)`
            );
        }
        
        // Update user's channel with view data
        const userChannel = guild.channels.cache.get(videoData.channelId);
        if (userChannel) {
            await userChannel.send(
                `📊 View Update: ${videoData.url}\n` +
                `Views: ${viewData.views.toLocaleString()}\n` +
                `Likes: ${viewData.likes.toLocaleString()}\n` +
                `Comments: ${viewData.comments.toLocaleString()}\n` +
                `Updated: ${new Date().toLocaleString()}`
            );
        }
        
        // Update leaderboard
        await updateLeaderboard(guild);
        
        console.log(`✅ Updated views for video from user ${videoData.userId}: ${viewData.views} views`);
        
    } catch (error) {
        console.error('Error processing queued video:', error);
        await logError(guild, error, `Processing queued video: ${videoData.url}`);
    }
};

const generateFinalReport = async (guild) => {
    try {
        console.log(`📊 Generating final report for ${guild.name}...`);
        
        // Wait for report generation delay
        setTimeout(async () => {
            try {
                const reportChannel = guild.channels.cache.find(ch => ch.name === CONFIG.EXECUTION_CHANNEL_NAME);
                if (!reportChannel) return;
                
                // Collect all data from ticket channels
                const ticketChannels = guild.channels.cache.filter(ch => 
                    ch.name.startsWith('ticket-') && ch.type === ChannelType.GuildText
                );
                
                const reportData = new Map();
                const topVideos = [];
                let totalQualifyingViews = 0;
                let totalVideos = 0;
                
                for (const channel of ticketChannels.values()) {
                    try {
                        const userId = channel.name.replace('ticket-', '');
                        const user = await client.users.fetch(userId).catch(() => null);
                        if (!user) continue;
                        
                        const messages = await channel.messages.fetch({ limit: 100 });
                        const userVideos = [];
                        const accounts = new Set();
                        
                        messages.forEach(message => {
                            // Extract account names
                            if (message.content.includes('@') && message.content.includes('instagram.com')) {
                                const accountMatch = message.content.match(/@(\w+)/);
                                if (accountMatch) accounts.add(accountMatch[1]);
                            }
                            
                            // Extract video data
                            const urlMatch = message.content.match(/(https:\/\/[^\s]+)/);
                            const viewMatch = message.content.match(/Views: ([\d,]+)/);
                            
                            if (urlMatch && viewMatch) {
                                const views = parseInt(viewMatch[1].replace(/,/g, ''));
                                const videoData = {
                                    url: urlMatch[1],
                                    views: views
                                };
                                
                                userVideos.push(videoData);
                                topVideos.push({ ...videoData, user: user.displayName });
                                
                                if (views >= CONFIG.MIN_VIEWS_THRESHOLD) {
                                    totalQualifyingViews += views;
                                }
                                totalVideos++;
                            }
                        });
                        
                        if (userVideos.length > 0) {
                            reportData.set(userId, {
                                user: user,
                                accounts: Array.from(accounts),
                                videos: userVideos,
                                totalViews: userVideos.reduce((sum, v) => v.views >= CONFIG.MIN_VIEWS_THRESHOLD ? sum + v.views : sum, 0),
                                qualifyingVideos: userVideos.filter(v => v.views >= CONFIG.MIN_VIEWS_THRESHOLD).length
                            });
                        }
                    } catch (error) {
                        console.error(`Error processing channel ${channel.name} for report:`, error);
                        continue;
                    }
                }
                
                // Sort top videos
                topVideos.sort((a, b) => b.views - a.views);
                const top5Videos = topVideos.slice(0, 5);
                
                // Create final report
                let reportText = `🏁 **FINAL CAMPAIGN REPORT**\n\n`;
                reportText += `📊 **Overall Statistics:**\n`;
                reportText += `• Total Videos: ${totalVideos}\n`;
                reportText += `• Total Qualifying Views (${CONFIG.MIN_VIEWS_THRESHOLD}+): ${totalQualifyingViews.toLocaleString()}\n`;
                reportText += `• Total Users: ${reportData.size}\n\n`;
                
                reportText += `🏆 **Top 5 Most Performing Videos:**\n`;
                top5Videos.forEach((video, index) => {
                    reportText += `${index + 1}. ${video.user} - ${formatNumber(video.views)} views\n`;
                    reportText += `   ${video.url}\n`;
                });
                
                reportText += `\n👥 **User Performance:**\n`;
                const sortedUsers = Array.from(reportData.entries())
                    .sort((a, b) => b[1].totalViews - a[1].totalViews);
                
                sortedUsers.forEach(([userId, data]) => {
                    reportText += `\n**${data.user.displayName}**\n`;
                    reportText += `• Accounts: ${data.accounts.join(', ')}\n`;
                    reportText += `• Total Qualifying Views: ${formatNumber(data.totalViews)}\n`;
                    reportText += `• Qualifying Videos: ${data.qualifyingVideos}/${data.videos.length}\n`;
                });
                
                // Split message if too long
                const maxLength = 1900;
                if (reportText.length > maxLength) {
                    const parts = [];
                    let currentPart = '';
                    const lines = reportText.split('\n');
                    
                    for (const line of lines) {
                        if (currentPart.length + line.length + 1 > maxLength) {
                            parts.push(currentPart);
                            currentPart = line;
                        } else {
                            currentPart += (currentPart ? '\n' : '') + line;
                        }
                    }
                    if (currentPart) parts.push(currentPart);
                    
                    for (let i = 0; i < parts.length; i++) {
                        await reportChannel.send(`**Final Report (${i + 1}/${parts.length})**\n${parts[i]}`);
                    }
                } else {
                    await reportChannel.send(reportText);
                }
                
                console.log(`✅ Final report generated for ${guild.name}`);
                
            } catch (error) {
                console.error('Error generating delayed final report:', error);
                await logError(guild, error, 'Generating final report');
            }
        }, CONFIG.REPORT_GENERATION_DELAY);
        
    } catch (error) {
        console.error('Error setting up final report:', error);
        await logError(guild, error, 'Setting up final report');
    }
};

const parseViewsFromChannel = async (channel, useCache = true) => {
    const cacheKey = `views_${channel.id}`;
    
    if (useCache && botCache.lastUpdate && 
        (Date.now() - botCache.lastUpdate) < CONFIG.CACHE_DURATION && 
        botCache.channelCounts.size > 0) {
        console.log('📦 Using cached channel counts');
        return new Map(botCache.channelCounts);
    }

    console.log('🔄 Fetching fresh channel counts...');
    const channelCounts = new Map();

    try {
        let allMessages = [];
        let lastMessageId = null;
        let fetchedCount = 0;

        while (fetchedCount < CONFIG.MAX_MESSAGES_PER_BATCH) {
            const options = { limit: Math.min(100, CONFIG.MAX_MESSAGES_PER_BATCH - fetchedCount) };
            if (lastMessageId) {
                options.before = lastMessageId;
            }

            const messages = await channel.messages.fetch(options);
            if (messages.size === 0) break;

            allMessages.push(...Array.from(messages.values()));
            lastMessageId = messages.last().id;
            fetchedCount += messages.size;

            if (messages.size === 100) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        console.log(`Fetched ${allMessages.length} messages from #${channel.name}`);

        allMessages.forEach(message => {
            if (message.embeds.length > 0) {
                const embed = message.embeds[0];

                if (embed.title && embed.title.includes('View Count Analysis')) {
                    let channelName = '';
                    let totalViews = 0;

                    if (embed.fields && embed.fields.length > 0) {
                        embed.fields.forEach(field => {
                            if (field.name === '**Channel**') {
                                channelName = field.value.replace(/[<>#]/g, '');
                            }
                            if (field.name === '**Total Views**') {
                                totalViews = parseInt(field.value.replace(/,/g, ''));
                            }
                        });
                    }

                    if (channelName && totalViews > 0) {
                        if (!channelName.startsWith('#')) {
                            channelName = '#' + channelName;
                        }

                        if (!channelCounts.has(channelName)) {
                            channelCounts.set(channelName, totalViews);
                            console.log(`Found latest count for ${channelName}: ${totalViews} views`);
                        }
                    }
                }
            }
        });

        botCache.channelCounts.clear();
        channelCounts.forEach((views, channel) => {
            botCache.channelCounts.set(channel, views);
        });
        botCache.lastUpdate = Date.now();
        console.log('📦 Cache updated with fresh data');

    } catch (error) {
        console.error('Error parsing views channel:', error);
    }

    return channelCounts;
};

const updateProgressVoiceChannel = async (guild, campaignName, currentViews, targetViews) => {
    try {
        const percentage = Math.min((currentViews / targetViews) * 100, 100);
        const channelName = `${CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX}${campaignName} (${formatNumber(currentViews)}/${formatNumber(targetViews)}) ${percentage.toFixed(1)}%`;

        let progressChannel = guild.channels.cache.find(ch => 
            ch.type === 2 && 
            ch.name.startsWith(CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX)
        );

        if (progressChannel) {
            if (progressChannel.name !== channelName) {
                await progressChannel.setName(channelName);
                console.log(`✅ Updated progress voice channel: ${channelName}`);
            }
        } else {
            progressChannel = await guild.channels.create({
                name: channelName,
                type: 2,
                permissionOverwrites: [
                    {
                        id: guild.roles.everyone,
                        deny: ['Connect', 'Speak'],
                    },
                ],
            });
            console.log(`✅ Created new progress voice channel: ${channelName}`);
        }

        botCache.progressSettings = {
            campaignName,
            target: targetViews,
            channelId: progressChannel.id
        };

        return progressChannel;
    } catch (error) {
        console.error('❌ Error updating progress voice channel:', error);
        return null;
    }
};

const getProgressSettings = async (guild, useCache = true) => {
    try {
        if (useCache && botCache.progressSettings) {
            console.log('📦 Using cached progress settings');
            return botCache.progressSettings;
        }

        console.log('🔄 Fetching fresh progress settings...');
        
        const progressChannel = guild.channels.cache.find(ch => 
            ch.type === 2 && 
            ch.name.startsWith(CONFIG.PROGRESS_VOICE_CHANNEL_PREFIX)
        );

        if (progressChannel) {
            const channelName = progressChannel.name;
            const match = channelName.match(/📊 Progress: (.+?) \([\d.,KM]+\/([\d.,KM]+)\)/);
            if (match) {
                const campaignName = match[1];
                const targetStr = match[2];

                let target = 0;
                if (targetStr.includes('M')) {
                    target = parseFloat(targetStr) * 1000000;
                } else if (targetStr.includes('K')) {
                    target = parseFloat(targetStr) * 1000;
                } else {
                    target = parseInt(targetStr.replace(/,/g, ''));
                }

                const settings = { 
                    campaignName, 
                    target,
                    channelId: progressChannel.id 
                };

                botCache.progressSettings = settings;
                return settings;
            }
        }

        return null;
    } catch (error) {
        console.error('❌ Error getting progress settings:', error);
        return null;
    }
};

const createExcelFile = async (results, channelName) => {
    const workbook = new ExcelJS.Workbook();
    const worksheet = workbook.addWorksheet('Instagram Data');

    worksheet.columns = [
        { header: 'Video URL', key: 'inputUrl', width: 50 },
        { header: 'Video ID', key: 'id', width: 20 },
        { header: 'Short Code', key: 'shortCode', width: 15 },
        { header: 'Username', key: 'ownerUsername', width: 20 },
        { header: 'Full Name', key: 'ownerFullName', width: 25 },
        { header: 'Video Views', key: 'videoPlayCount', width: 15 },
        { header: 'Likes Count', key: 'likesCount', width: 15 },
        { header: 'Comments Count', key: 'commentsCount', width: 15 },
        { header: 'Timestamp', key: 'timestamp', width: 20 },
        { header: 'Video Duration', key: 'videoDuration', width: 15 }
    ];

    results.forEach(item => {
        worksheet.addRow({
            inputUrl: item.inputUrl,
            id: item.id,
            shortCode: item.shortCode,
            ownerUsername: item.ownerUsername,
            ownerFullName: item.ownerFullName,
            videoPlayCount: item.videoPlayCount || 0,
            likesCount: item.likesCount || 0,
            commentsCount: item.commentsCount || 0,
            timestamp: item.timestamp,
            videoDuration: item.videoDuration || 0
        });
    });

    worksheet.getRow(1).font = { bold: true };
    worksheet.getRow(1).fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: 'FFE0E0E0' }
    };

    const buffer = await workbook.xlsx.writeBuffer();
    return buffer;
};

const withTimeoutProtection = async (interaction, handler) => {
    const startTime = Date.now();
    
    const warningTimeout = setTimeout(async () => {
        if (!interaction.replied && !interaction.deferred) return;
        
        try {
            await interaction.editReply({
                content: '⚠️ This operation is taking longer than expected. Please wait...',
                embeds: []
            });
        } catch (error) {
            console.log('Could not send timeout warning:', error.message);
        }
    }, 12 * 60 * 1000);

    try {
        await handler();
        clearTimeout(warningTimeout);
    } catch (error) {
        clearTimeout(warningTimeout);
        
        const elapsedTime = Date.now() - startTime;
        console.error(`Command failed after ${Math.round(elapsedTime / 1000)}s:`, error);
        
        if (error.code === 10062 || error.code === 40060) {
            const executionChannel = interaction.guild?.channels.cache.find(ch => 
                ch.name === CONFIG.EXECUTION_CHANNEL_NAME
            );
            
            if (executionChannel) {
                await executionChannel.send({
                    content: `❌ Command timeout for ${interaction.user.tag}: ${error.message}`,
                    embeds: [new EmbedBuilder()
                        .setTitle('⏰ Command Timeout')
                        .setDescription(`The \`${interaction.commandName}\` command took too long to execute.`)
                        .addFields(
                            { name: 'User', value: interaction.user.tag, inline: true },
                            { name: 'Duration', value: `${Math.round(elapsedTime / 1000)}s`, inline: true },
                            { name: 'Error', value: error.message, inline: false }
                        )
                        .setColor(0xFF0000)
                        .setTimestamp()
                    ]
                });
            }
        } else {
            try {
                if (!interaction.replied && !interaction.deferred) {
                    await interaction.reply('❌ An error occurred while processing your command.');
                } else {
                    await interaction.editReply('❌ An error occurred while processing your command.');
                }
            } catch (replyError) {
                console.error('Could not send error reply:', replyError.message);
            }
        }
        
        throw error;
    }
};

// Slash command definitions
const commands = [
    // User commands
    new SlashCommandBuilder()
        .setName('verify')
        .setDescription('Verify your Instagram account')
        .addStringOption(option =>
            option.setName('username')
                .setDescription('Your Instagram username (without @)')
                .setRequired(true))
        .setDefaultMemberPermissions(null),

    new SlashCommandBuilder()
        .setName('submit')
        .setDescription('Submit an Instagram Reel link')
        .addStringOption(option =>
            option.setName('link')
                .setDescription('Instagram Reel URL')
                .setRequired(true))
        .setDefaultMemberPermissions(null),

    new SlashCommandBuilder()
        .setName('list')
        .setDescription('List your verified accounts and submitted videos')
        .setDefaultMemberPermissions(null),

    // Campaign manager commands
    new SlashCommandBuilder()
        .setName('progressbar')
        .setDescription('Create/update progress voice channel')
        .addStringOption(option =>
            option.setName('campaign')
                .setDescription('Campaign name')
                .setRequired(true))
        .addIntegerOption(option =>
            option.setName('target')
                .setDescription('Target view count')
                .setRequired(true))
        .setDefaultMemberPermissions(null),

    new SlashCommandBuilder()
        .setName('updateprogress')
        .setDescription('Manually update progress voice channel with current totals')
        .setDefaultMemberPermissions(null),

    // Admin commands
    new SlashCommandBuilder()
        .setName('stats')
        .setDescription('Get comprehensive statistics in CSV format')
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

    new SlashCommandBuilder()
        .setName('status')
        .setDescription('Check bot status and health')
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator),

    new SlashCommandBuilder()
        .setName('clearcache')
        .setDescription('Clear bot cache and force refresh of data')
        .setDefaultMemberPermissions(PermissionFlagsBits.Administrator)
];

// Command handlers
client.on('interactionCreate', async interaction => {
    if (interaction.isChatInputCommand()) {
        const { commandName } = interaction;

        try {
            const permissionCheck = checkPermissions(interaction, commandName);
            
            if (!permissionCheck.allowed) {
                const embed = new EmbedBuilder()
                    .setTitle('❌ Access Denied')
                    .setDescription(permissionCheck.reason)
                    .setColor(0xFF0000)
                    .setTimestamp();

                await interaction.reply({ embeds: [embed], ephemeral: true });
                return;
            }

            console.log(`✅ Command ${commandName} used by ${interaction.user.tag} (${permissionCheck.reason})`);

            // Execute commands with timeout protection
            switch (commandName) {
                case 'verify':
                    await withTimeoutProtection(interaction, () => handleVerify(interaction));
                    break;
                case 'submit':
                    await withTimeoutProtection(interaction, () => handleSubmit(interaction));
                    break;
                case 'list':
                    await withTimeoutProtection(interaction, () => handleList(interaction));
                    break;
                case 'progressbar':
                    await withTimeoutProtection(interaction, () => handleProgressBar(interaction));
                    break;
                case 'updateprogress':
                    await withTimeoutProtection(interaction, () => handleUpdateProgress(interaction));
                    break;
                case 'stats':
                    await withTimeoutProtection(interaction, () => handleStats(interaction));
                    break;
                case 'status':
                    await handleStatus(interaction);
                    break;
                case 'clearcache':
                    await handleClearCache(interaction);
                    break;
            }
        } catch (error) {
            console.error(`❌ Error handling command ${commandName}:`, error);
            await logError(interaction.guild, error, `Command: ${commandName}`);
        }
    } else if (interaction.isButton()) {
        if (interaction.customId === 'verify_account') {
            await handleVerifyButton(interaction);
        }
    }
});

// Individual command handlers
const handleVerify = async (interaction) => {
    const username = interaction.options.getString('username').replace('@', '');
    
    await interaction.deferReply({ ephemeral: true });
    
    try {
        // Check for duplicate account
        const duplicateCheck = await checkDuplicateAccount(interaction.guild, username);
        if (duplicateCheck.isDuplicate) {
            await interaction.editReply({
                content: `❌ Account @${username} is already linked to ${duplicateCheck.existingUser ? duplicateCheck.existingUser.displayName : 'another user'}.`,
                ephemeral: true
            });
            return;
        }
        
        // Generate verification code
        const verificationCode = generateVerificationCode();
        
        // Store verification attempt in cache
        botCache.verifiedAccounts.set(`${interaction.user.id}_pending`, {
            username: username,
            code: verificationCode,
            timestamp: Date.now()
        });
        
        // Create verification embed with button
        const embed = new EmbedBuilder()
            .setTitle('📱 Account Verification')
            .setDescription(`To verify your Instagram account @${username}, please follow these steps:`)
            .addFields(
                { 
                    name: '1️⃣ Add Code to Bio', 
                    value: `Add this code to your Instagram bio: \`${verificationCode}\``, 
                    inline: false 
                },
                { 
                    name: '2️⃣ Click Verify', 
                    value: 'Click the "Verify Account" button below once you\'ve added the code to your bio.', 
                    inline: false 
                },
                { 
                    name: '⚠️ Important', 
                    value: 'The code must be visible in your bio. You can remove it after verification is complete.', 
                    inline: false 
                }
            )
            .setColor(0x0099FF)
            .setTimestamp();
        
        const button = new ButtonBuilder()
            .setCustomId('verify_account')
            .setLabel('Verify Account')
            .setStyle(ButtonStyle.Primary)
            .setEmoji('✅');
        
        const row = new ActionRowBuilder().addComponents(button);
        
        await interaction.editReply({
            embeds: [embed],
            components: [row],
            ephemeral: true
        });
        
    } catch (error) {
        console.error('Error in verify command:', error);
        await logError(interaction.guild, error, 'Verify command');
        await interaction.editReply('❌ An error occurred during verification setup.');
    }
};

const handleVerifyButton = async (interaction) => {
    await interaction.deferUpdate();
    
    try {
        // Get pending verification
        const pendingVerification = botCache.verifiedAccounts.get(`${interaction.user.id}_pending`);
        if (!pendingVerification) {
            await interaction.followUp({
                content: '❌ No pending verification found. Please use `/verify` command first.',
                ephemeral: true
            });
            return;
        }
        
        // Check if verification is still valid (15 minutes)
        if (Date.now() - pendingVerification.timestamp > 15 * 60 * 1000) {
            botCache.verifiedAccounts.delete(`${interaction.user.id}_pending`);
            await interaction.followUp({
                content: '❌ Verification expired. Please use `/verify` command again.',
                ephemeral: true
            });
            return;
        }
        
        // Scrape Instagram bio
        await interaction.followUp({
            content: '🔍 Checking your Instagram bio...',
            ephemeral: true
        });
        
        const bioData = await scrapeInstagramBio(pendingVerification.username);
        if (!bioData || !bioData.biography) {
            await interaction.editReply({
                content: '❌ Could not access your Instagram profile. Make sure your account is public.',
                ephemeral: true
            });
            return;
        }
        
        // Check if verification code is in bio
        if (!bioData.biography.includes(pendingVerification.code)) {
            await interaction.editReply({
                content: `❌ Verification code \`${pendingVerification.code}\` not found in your bio. Please add it and try again.`,
                ephemeral: true
            });
            return;
        }
        
        // Verification successful - create or update user channel
        let userChannel = await getUserTicketChannel(interaction.guild, interaction.user.id);
        let isNewAccount = true;
        let accountNumber = 1;
        
        if (userChannel) {
            // Get existing accounts
            const existingAccounts = await getVerifiedAccounts(userChannel);
            accountNumber = existingAccounts.length + 1;
            isNewAccount = false;
        } else {
            // Create new user channel
            userChannel = await createUserTicketChannel(interaction.guild, interaction.user);
        }
        
        // Add account info to user channel
        const accountInfo = `${accountNumber}. @${pendingVerification.username} - https://instagram.com/${pendingVerification.username}\n` +
            `   Name: ${bioData.fullName || 'N/A'}\n` +
            `   Followers: ${bioData.followersCount?.toLocaleString() || 'N/A'}\n` +
            `   Posts: ${bioData.postsCount?.toLocaleString() || 'N/A'}\n` +
            `   Verified: ${new Date().toLocaleString()}\n`;
        
        await userChannel.send(accountInfo);
        
        // Clean up pending verification
        botCache.verifiedAccounts.delete(`${interaction.user.id}_pending`);
        
        // Success message
        const successMessage = isNewAccount 
            ? `✅ Account verified successfully!\n\n**@${pendingVerification.username}** has been linked to your Discord account.\n\nYou can now use \`/submit\` to submit Instagram Reels and \`/list\` to view your accounts and videos.`
            : `✅ Additional account verified!\n\n**@${pendingVerification.username}** has been added as your ${accountNumber}${accountNumber === 2 ? 'nd' : accountNumber === 3 ? 'rd' : 'th'} account.\n\nYou can now submit videos from this account using \`/submit\`.`;
        
        await interaction.editReply({
            content: successMessage,
            components: [],
            ephemeral: true
        });
        
        console.log(`✅ Account @${pendingVerification.username} verified for user ${interaction.user.tag}`);
        
    } catch (error) {
        console.error('Error in verify button handler:', error);
        await logError(interaction.guild, error, 'Verify button handler');
        await interaction.editReply({
            content: '❌ An error occurred during verification.',
            components: [],
            ephemeral: true
        });
    }
};

const handleSubmit = async (interaction) => {
    const link = interaction.options.getString('link');
    
    await interaction.deferReply({ ephemeral: true });
    
    try {
        // Validate Instagram URL
        const instagramUrlRegex = /https?:\/\/(?:www\.)?instagram\.com\/(?:reel|p)\/[A-Za-z0-9_-]+/;
        if (!instagramUrlRegex.test(link)) {
            await interaction.editReply('❌ Please provide a valid Instagram Reel URL.');
            return;
        }
        
        // Get user's channel
        const userChannel = await getUserTicketChannel(interaction.guild, interaction.user.id);
        if (!userChannel) {
            await interaction.editReply('❌ No verified account found. Please use `/verify` to link your Instagram account first.');
            return;
        }
        
        // Get verified accounts from user channel
        const verifiedAccounts = await getVerifiedAccounts(userChannel);
        if (verifiedAccounts.length === 0) {
            await interaction.editReply('❌ No verified accounts found. Please use `/verify` to link your Instagram account first.');
            return;
        }
        
        // Extract username from URL and verify ownership
        const urlPattern = /instagram\.com\/(?:reel|p)\/([A-Za-z0-9_-]+)/;
        const match = link.match(urlPattern);
        if (!match) {
            await interaction.editReply('❌ Could not extract information from the provided URL.');
            return;
        }
        
        // For now, accept any video from verified accounts
        // In a real implementation, you might want to verify the video belongs to one of their accounts
        
        // Add video to user channel
        const videoSubmission = `📹 Video submitted: ${link}\n` +
            `   Submitted by: ${interaction.user.displayName}\n` +
            `   Submitted at: ${new Date().toLocaleString()}\n`;
        
        await userChannel.send(videoSubmission);
        
        await interaction.editReply('✅ Video submitted successfully! It will be included in the next view count update.');
        
        console.log(`📹 Video submitted by ${interaction.user.tag}: ${link}`);
        
    } catch (error) {
        console.error('Error in submit command:', error);
        await logError(interaction.guild, error, 'Submit command');
        await interaction.editReply('❌ An error occurred while submitting your video.');
    }
};

const handleList = async (interaction) => {
    await interaction.deferReply({ ephemeral: true });
    
    try {
        // Get user's channel
        const userChannel = await getUserTicketChannel(interaction.guild, interaction.user.id);
        if (!userChannel) {
            await interaction.editReply('❌ No verified account found. Please use `/verify` to link your Instagram account first.');
            return;
        }
        
        // Get all data from user channel
        const messages = await userChannel.messages.fetch({ limit: 100 });
        const accounts = [];
        const videos = [];
        
        messages.forEach(message => {
            const content = message.content;
            
            // Extract accounts
            if (content.includes('@') && content.includes('instagram.com') && !content.includes('📹')) {
                const lines = content.split('\n');
                const accountLine = lines[0];
                if (accountLine.includes('@')) {
                    accounts.push(accountLine);
                }
            }
            
            // Extract videos
            if (content.includes('📹 Video submitted:')) {
                const urlMatch = content.match(/(https:\/\/[^\s]+)/);
                const dateMatch = content.match(/Submitted at: (.+)/);
                if (urlMatch) {
                    videos.push({
                        url: urlMatch[1],
                        date: dateMatch ? dateMatch[1] : 'Unknown'
                    });
                }
            }
        });
        
        // Create response
        let responseText = `📱 **Your Verified Accounts:**\n`;
        if (accounts.length > 0) {
            accounts.forEach(account => {
                responseText += `${account}\n`;
            });
        } else {
            responseText += 'No verified accounts found.\n';
        }
        
        responseText += `\n📹 **Your Submitted Videos (${videos.length}):**\n`;
        if (videos.length > 0) {
            videos.slice(0, 10).forEach((video, index) => {
                responseText += `${index + 1}. ${video.url}\n   Submitted: ${video.date}\n`;
            });
            
            if (videos.length > 10) {
                responseText += `... and ${videos.length - 10} more videos\n`;
            }
        } else {
            responseText += 'No videos submitted yet.\n';
        }
        
        await interaction.editReply(responseText);
        
    } catch (error) {
        console.error('Error in list command:', error);
        await logError(interaction.guild, error, 'List command');
        await interaction.editReply('❌ An error occurred while retrieving your data.');
    }
};

const handleProgressBar = async (interaction) => {
    const campaignName = interaction.options.getString('campaign');
    const targetViews = interaction.options.getInteger('target');

    await interaction.deferReply();

    try {
        await interaction.editReply('🔄 Setting up campaign progress tracking...');

        // Create/update progress voice channel with 0 initial views
        const progressChannel = await updateProgressVoiceChannel(
            interaction.guild, 
            campaignName, 
            0, 
            targetViews
        );

        if (progressChannel) {
            const embed = new EmbedBuilder()
                .setTitle('🎯 Campaign Progress Tracker Created')
                .setColor(0x00FF00)
                .addFields(
                    { 
                        name: `**${campaignName}**`, 
                        value: `Target: ${targetViews.toLocaleString()} views`, 
                        inline: false 
                    },
                    {
                        name: '**Voice Channel**',
                        value: `Progress tracking: ${progressChannel}`,
                        inline: false
                    },
                    {
                        name: '**Automated Updates**',
                        value: 'The bot will now automatically update views every hour and track progress.',
                        inline: false
                    }
                )
                .setTimestamp();

            await interaction.editReply({ embeds: [embed] });
            
            // Initialize automated view updates
            await buildViewUpdateQueue(interaction.guild);
            console.log(`🎯 Campaign "${campaignName}" started with target ${targetViews.toLocaleString()} views`);
        } else {
            await interaction.editReply('❌ Failed to create/update progress voice channel.');
        }

    } catch (error) {
        console.error('Error in progressbar command:', error);
        await logError(interaction.guild, error, 'Progressbar command');
        await interaction.editReply('❌ An error occurred while setting up progress tracking.');
    }
};

const handleUpdateProgress = async (interaction) => {
    await interaction.deferReply();

    try {
        const progressSettings = await getProgressSettings(interaction.guild, false);

        if (!progressSettings) {
            await interaction.editReply('❌ No progress tracker found. Use `/progressbar` to create one first.');
            return;
        }

        // Calculate total views from all user channels
        const ticketChannels = interaction.guild.channels.cache.filter(ch => 
            ch.name.startsWith('ticket-') && ch.type === ChannelType.GuildText
        );
        
        let totalViews = 0;
        let totalVideos = 0;

        for (const channel of ticketChannels.values()) {
            try {
                const messages = await channel.messages.fetch({ limit: 100 });
                
                messages.forEach(message => {
                    const viewMatch = message.content.match(/Views: ([\d,]+)/);
                    if (viewMatch) {
                        const views = parseInt(viewMatch[1].replace(/,/g, ''));
                        if (views >= CONFIG.MIN_VIEWS_THRESHOLD) {
                            totalViews += views;
                        }
                        totalVideos++;
                    }
                });
            } catch (error) {
                console.error(`Error processing channel ${channel.name}:`, error);
                continue;
            }
        }

        const progressChannel = await updateProgressVoiceChannel(
            interaction.guild,
            progressSettings.campaignName,
            totalViews,
            progressSettings.target
        );

        if (progressChannel) {
            const percentage = Math.min((totalViews / progressSettings.target) * 100, 100);

            const embed = new EmbedBuilder()
                .setTitle('🔄 Progress Updated')
                .setColor(0x0099FF)
                .addFields(
                    { 
                        name: `**${progressSettings.campaignName}**`, 
                        value: `${totalViews.toLocaleString()}/${progressSettings.target.toLocaleString()} views (${percentage.toFixed(1)}%)`, 
                        inline: false 
                    },
                    {
                        name: '**Voice Channel**',
                        value: `Updated: ${progressChannel}`,
                        inline: false
                    },
                    {
                        name: '**Statistics**',
                        value: `${totalVideos} total videos • ${ticketChannels.size} active users`,
                        inline: false
                    }
                )
                .setTimestamp();

            await interaction.editReply({ embeds: [embed] });
            
            // Update leaderboard
            await updateLeaderboard(interaction.guild, true);
        } else {
            await interaction.editReply('❌ Failed to update progress voice channel.');
        }

    } catch (error) {
        console.error('Error updating progress:', error);
        await logError(interaction.guild, error, 'Update progress command');
        await interaction.editReply('❌ An error occurred while updating progress.');
    }
};

const handleStats = async (interaction) => {
    await interaction.deferReply();

    try {
        await interaction.editReply('📊 Generating comprehensive statistics... This may take a few minutes.');

        // Collect all video URLs from all user channels
        const ticketChannels = interaction.guild.channels.cache.filter(ch => 
            ch.name.startsWith('ticket-') && ch.type === ChannelType.GuildText
        );
        
        const allVideos = [];
        
        for (const channel of ticketChannels.values()) {
            try {
                const userId = channel.name.replace('ticket-', '');
                const user = await client.users.fetch(userId).catch(() => null);
                
                const messages = await channel.messages.fetch({ limit: 100 });
                const userAccounts = [];
                
                messages.forEach(message => {
                    // Extract account info
                    if (message.content.includes('@') && message.content.includes('instagram.com') && !message.content.includes('📹')) {
                        const accountMatch = message.content.match(/@(\w+)/);
                        if (accountMatch && !userAccounts.includes(accountMatch[1])) {
                            userAccounts.push(accountMatch[1]);
                        }
                    }
                    
                    // Extract video URLs
                    const urlMatch = message.content.match(/(https:\/\/(?:www\.)?instagram\.com\/(?:reel|p)\/[A-Za-z0-9_-]+)/);
                    if (urlMatch) {
                        allVideos.push({
                            url: urlMatch[1],
                            userId: userId,
                            userName: user ? user.displayName : 'Unknown',
                            userAccounts: userAccounts.join(', ')
                        });
                    }
                });
            } catch (error) {
                console.error(`Error processing channel ${channel.name}:`, error);
                continue;
            }
        }

        if (allVideos.length === 0) {
            await interaction.editReply('❌ No videos found to analyze.');
            return;
        }

        await interaction.editReply(`🔍 Found ${allVideos.length} videos. Processing with Apify...`);

        // Process videos with Apify
        const batchSize = 50; // Process in batches to avoid memory issues
        const allResults = [];
        
        for (let i = 0; i < allVideos.length; i += batchSize) {
            const batch = allVideos.slice(i, i + batchSize);
            const urls = batch.map(v => v.url);
            
            try {
                const input = {
                    addParentData: false,
                    directUrls: urls,
                    enhanceUserSearchWithFacebookPage: false,
                    isUserReelFeedURL: true,
                    isUserTaggedFeedURL: false,
                    resultsLimit: 1,
                    resultsType: "posts",
                    searchLimit: 1
                };
                
                const run = await apifyViewsClient.task(CONFIG.APIFY_VIEWS_TASK_ID).call(input);
                const { items } = await apifyViewsClient.dataset(run.defaultDatasetId).listItems();
                
                // Merge with user data
                items.forEach(item => {
                    const matchingVideo = batch.find(v => v.url === item.inputUrl);
                    if (matchingVideo) {
                        allResults.push({
                            ...item,
                            discordUserId: matchingVideo.userId,
                            discordUserName: matchingVideo.userName,
                            linkedAccounts: matchingVideo.userAccounts
                        });
                    }
                });
                
                await interaction.editReply(`🔍 Processed ${Math.min(i + batchSize, allVideos.length)}/${allVideos.length} videos...`);
                
                // Add delay to avoid rate limiting
                if (i + batchSize < allVideos.length) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            } catch (error) {
                console.error(`Error processing batch ${i}-${i + batchSize}:`, error);
                continue;
            }
        }

        if (allResults.length === 0) {
            await interaction.editReply('❌ No valid data retrieved from Apify.');
            return;
        }

        await interaction.editReply('📊 Creating Excel file...');

        // Create enhanced Excel file with Discord user data
        const workbook = new ExcelJS.Workbook();
        const worksheet = workbook.addWorksheet('Complete Instagram Stats');

        worksheet.columns = [
            { header: 'Discord User', key: 'discordUserName', width: 20 },
            { header: 'Discord User ID', key: 'discordUserId', width: 20 },
            { header: 'Linked IG Accounts', key: 'linkedAccounts', width: 30 },
            { header: 'Video URL', key: 'inputUrl', width: 50 },
            { header: 'Video ID', key: 'id', width: 20 },
            { header: 'Short Code', key: 'shortCode', width: 15 },
            { header: 'IG Username', key: 'ownerUsername', width: 20 },
            { header: 'IG Full Name', key: 'ownerFullName', width: 25 },
            { header: 'Video Views', key: 'videoPlayCount', width: 15 },
            { header: 'Likes Count', key: 'likesCount', width: 15 },
            { header: 'Comments Count', key: 'commentsCount', width: 15 },
            { header: 'Timestamp', key: 'timestamp', width: 20 },
            { header: 'Video Duration', key: 'videoDuration', width: 15 }
        ];

        allResults.forEach(item => {
            worksheet.addRow({
                discordUserName: item.discordUserName || 'Unknown',
                discordUserId: item.discordUserId || 'Unknown',
                linkedAccounts: item.linkedAccounts || 'N/A',
                inputUrl: item.inputUrl,
                id: item.id,
                shortCode: item.shortCode,
                ownerUsername: item.ownerUsername,
                ownerFullName: item.ownerFullName,
                videoPlayCount: item.videoPlayCount || 0,
                likesCount: item.likesCount || 0,
                commentsCount: item.commentsCount || 0,
                timestamp: item.timestamp,
                videoDuration: item.videoDuration || 0
            });
        });

        // Style the header
        worksheet.getRow(1).font = { bold: true };
        worksheet.getRow(1).fill = {
            type: 'pattern',
            pattern: 'solid',
            fgColor: { argb: 'FFE0E0E0' }
        };

        const buffer = await workbook.xlsx.writeBuffer();
        const attachment = new AttachmentBuilder(buffer, { 
            name: `complete_instagram_stats_${new Date().toISOString().split('T')[0]}.xlsx` 
        });

        // Create summary embed
        const totalViews = allResults.reduce((sum, item) => sum + (item.videoPlayCount || 0), 0);
        const totalLikes = allResults.reduce((sum, item) => sum + (item.likesCount || 0), 0);
        const uniqueUsers = new Set(allResults.map(item => item.discordUserId)).size;

        const embed = new EmbedBuilder()
            .setTitle('📊 Complete Statistics Export')
            .setColor(0x0099FF)
            .addFields(
                { name: '📹 Total Videos', value: allResults.length.toString(), inline: true },
                { name: '👥 Discord Users', value: uniqueUsers.toString(), inline: true },
                { name: '👀 Total Views', value: totalViews.toLocaleString(), inline: true },
                { name: '❤️ Total Likes', value: totalLikes.toLocaleString(), inline: true },
                { name: '📊 Average Views/Video', value: Math.round(totalViews / allResults.length).toLocaleString(), inline: true },
                { name: '📈 Export Date', value: new Date().toLocaleDateString(), inline: true }
            )
            .setTimestamp();

        await interaction.editReply({ 
            content: '✅ Statistics export completed!',
            embeds: [embed], 
            files: [attachment] 
        });

        console.log(`📊 Stats export completed: ${allResults.length} videos, ${uniqueUsers} users`);

    } catch (error) {
        console.error('Error in stats command:', error);
        await logError(interaction.guild, error, 'Stats command');
        await interaction.editReply('❌ An error occurred while generating statistics.');
    }
};

const handleStatus = async (interaction) => {
    await interaction.deferReply();

    const memUsage = process.memoryUsage();
    const cacheInfo = botCache.getMemoryUsage();

    const embed = new EmbedBuilder()
        .setTitle('🤖 Bot Status')
        .setColor(0x00FF00)
        .addFields(
            { name: '🟢 Status', value: 'Online', inline: true },
            { name: '📊 Heap Used', value: `${Math.round(memUsage.heapUsed / 1024 / 1024)} MB`, inline: true },
            { name: '📊 Total Memory', value: `${Math.round(memUsage.rss / 1024 / 1024)} MB`, inline: true },
            { name: '⏱️ Uptime', value: `${Math.floor(process.uptime() / 3600)}h ${Math.floor((process.uptime() % 3600) / 60)}m`, inline: true },
            { name: '📡 Ping', value: `${client.ws.ping}ms`, inline: true },
            { name: '🏠 Guilds', value: client.guilds.cache.size.toString(), inline: true },
            { name: '📦 Cache Stats', value: `Channels: ${cacheInfo.channelCounts}\nUsers: ${cacheInfo.userChannels}\nQueue: ${cacheInfo.viewUpdateQueue}`, inline: false },
            { name: '🎯 Progress Settings', value: cacheInfo.progressSettings, inline: true },
            { name: '🕒 Last Update', value: botCache.lastUpdate ? new Date(botCache.lastUpdate).toLocaleString() : 'Never', inline: true },
            { name: '🔧 Environment', value: 'Koyeb Hosting (512MB RAM)', inline: false }
        )
        .setTimestamp();

    await interaction.editReply({ embeds: [embed] });
};

const handleClearCache = async (interaction) => {
    await interaction.deferReply();

    try {
        const beforeMemory = process.memoryUsage().heapUsed / 1024 / 1024;
        const beforeCacheInfo = botCache.getMemoryUsage();

        // Clear bot cache
        botCache.clear();

        // Force garbage collection if available
        if (global.gc) {
            global.gc();
        }

        const afterMemory = process.memoryUsage().heapUsed / 1024 / 1024;
        const memorySaved = beforeMemory - afterMemory;

        const embed = new EmbedBuilder()
            .setTitle('🗑️ Cache Cleared')
            .setColor(0x00FF00)
            .addFields(
                { name: '**Before**', value: `Channels: ${beforeCacheInfo.channelCounts}\nUsers: ${beforeCacheInfo.userChannels}\nQueue: ${beforeCacheInfo.viewUpdateQueue}`, inline: true },
                { name: '**Memory**', value: `Before: ${beforeMemory.toFixed(1)} MB\nAfter: ${afterMemory.toFixed(1)} MB\nFreed: ${Math.max(0, memorySaved).toFixed(1)} MB`, inline: true },
                { name: '**Status**', value: 'All cached data cleared. Next operations will fetch fresh data.', inline: false }
            )
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
        console.log(`🗑️ Cache cleared by ${interaction.user.tag} - Freed ${memorySaved.toFixed(1)} MB`);

    } catch (error) {
        console.error('Error clearing cache:', error);
        await logError(interaction.guild, error, 'Clear cache command');
        await interaction.editReply('❌ An error occurred while clearing cache.');
    }
};

// Bot ready event
client.once('ready', async () => {
    console.log(`✅ Bot is ready! Logged in as ${client.user.tag}`);
    console.log(`🏠 Connected to ${client.guilds.cache.size} guilds`);

    // Register slash commands
    try {
        console.log('🔄 Started refreshing application (/) commands.');
        await client.application.commands.set(commands);
        console.log('✅ Successfully reloaded application (/) commands.');
    } catch (error) {
        console.error('❌ Error registering commands:', error);
    }

    // Start automated view updater
    setInterval(automatedViewUpdater, CONFIG.VIEW_UPDATE_INTERVAL);
    console.log(`🔄 Automated view updater started (${CONFIG.VIEW_UPDATE_INTERVAL / 1000 / 60} minute intervals)`);

    // Clean up old cache periodically
    setInterval(() => {
        const now = Date.now();
        
        // Clean expired verification attempts
        for (const [key, data] of botCache.verifiedAccounts.entries()) {
            if (key.includes('_pending') && now - data.timestamp > 15 * 60 * 1000) {
                botCache.verifiedAccounts.delete(key);
                console.log(`🗑️ Cleaned expired verification for ${key}`);
            }
        }
        
        // Clean URL cache
        const urlCacheKeys = [...botCache.urlCache.keys()];
        urlCacheKeys.forEach(key => {
            if (key.endsWith('_timestamp')) {
                const timestamp = botCache.urlCache.get(key);
                if (now - timestamp > CONFIG.CACHE_DURATION * 2) {
                    const baseKey = key.replace('_timestamp', '');
                    botCache.urlCache.delete(key);
                    botCache.urlCache.delete(baseKey);
                    console.log(`🗑️ Cleaned expired cache for ${baseKey}`);
                }
            }
        });

        // Force garbage collection if memory usage is high
        const memUsage = process.memoryUsage();
        if (global.gc && memUsage.heapUsed > 400 * 1024 * 1024) { // 400MB threshold
            global.gc();
            console.log(`🗑️ Performed garbage collection - freed ${(memUsage.heapUsed - process.memoryUsage().heapUsed) / 1024 / 1024}MB`);
        }
    }, 10 * 60 * 1000); // Every 10 minutes
});

// Enhanced error handling
process.on('unhandledRejection', error => {
    console.error('❌ Unhandled promise rejection:', error);
    
    if (client?.isReady()) {
        client.guilds.cache.forEach(guild => {
            logError(guild, error, 'Unhandled Promise Rejection').catch(() => {});
        });
    }
});

process.on('uncaughtException', error => {
    console.error('❌ Uncaught exception:', error);
    
    if (client?.isReady()) {
        client.guilds.cache.forEach(guild => {
            logError(guild, error, 'Uncaught Exception - Bot Restarting').catch(() => {});
        });
    }
    
    setTimeout(() => {
        process.exit(1);
    }, 1000);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('📴 Received SIGTERM, shutting down gracefully...');
    botCache.clear();
    client?.destroy();
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('📴 Received SIGINT, shutting down gracefully...');
    botCache.clear();
    client?.destroy();
    process.exit(0);
});

// Start Express server first
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🌐 Health check server running on port ${PORT}`);
    console.log(`📊 Available endpoints:`);
    console.log(`   GET  / - Basic status`);
    console.log(`   GET  /health - Detailed health check`);
    console.log(`   POST /gc - Force garbage collection`);
});

// Environment validation
if (!process.env.DISCORD_TOKEN) {
    console.error('❌ DISCORD_TOKEN environment variable is required');
    process.exit(1);
}

if (!process.env.APIFY_VIEWS_TOKEN) {
    console.error('❌ APIFY_VIEWS_TOKEN environment variable is required');
    process.exit(1);
}

if (!process.env.APIFY_VERIFY_TOKEN) {
    console.error('❌ APIFY_VERIFY_TOKEN environment variable is required');
    process.exit(1);
}

// Login to Discord
client.login(process.env.DISCORD_TOKEN).catch(error => {
    console.error('❌ Failed to login:', error);
    process.exit(1);
});
